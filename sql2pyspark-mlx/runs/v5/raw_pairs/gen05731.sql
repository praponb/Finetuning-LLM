WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE supplier_name IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        city,
        country_supplier_name_key,
        resolved_city,
        LAG(rating, 2) OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS prev_rating,
        LAST_VALUE(rating) IGNORE NULLS OVER w_part AS last_known_rating
    FROM cleaned
    WINDOW w_part AS (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        country_supplier_name_key,
        resolved_city,
        prev_rating,
        CONCAT(city, ' ', supplier_name) AS city_supplier_name,
        COALESCE(city, 'NONE') AS city_clean
    FROM metrics
    WHERE UPPER(supplier_name) LIKE '%ACI%'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.country_supplier_name_key,
        s.prev_rating,
        s.city_supplier_name,
        s.city_clean,
        x.product_stock_qty,
        x.product_cost
    FROM cleaned_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', supplier_name, city) AS supplier_name_city_key,
        COALESCE(city, 'NONE') AS city_filled
    FROM latest_match
    WHERE city IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_cost ASC NULLS LAST, id ASC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(product_stock_qty, 1) OVER (ORDER BY product_stock_qty ASC, id ASC) AS prev_product_stock_qty,
        SUM(product_cost) OVER (PARTITION BY city ORDER BY product_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_product_cost,
        AVG(product_stock_qty) OVER (PARTITION BY city ORDER BY product_cost ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_stock_qty_avg_3
    FROM latest
)
SELECT
    id,
    city,
    country_supplier_name_key,
    prev_rating,
    city_supplier_name,
    city_clean,
    supplier_name_city_key,
    city_filled,
    rn,
    CASE WHEN city RLIKE '^(Singapore|Bangkok)$' THEN 'EXTENDED' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS city_zone
FROM enriched
ORDER BY rn DESC
