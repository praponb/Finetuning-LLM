WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        rating,
        ROUND(rating * 1.1, 2) AS rating_adjusted,
        CONCAT(supplier_name, ' ', country) AS supplier_name_country,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE LENGTH(supplier_name) > 12
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.category,
        x.stock_qty
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.category, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        stock_qty * rating AS stock_qty_times_rating,
        COALESCE(category, 'DIRECT') AS resolved_category
    FROM with_last
    WHERE rating_adjusted < 25
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base_suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, category ORDER BY stock_qty DESC, id) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price < 720
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        rating,
        rating_adjusted,
        category,
        stock_qty,
        resolved_category,
        CONCAT(country, '/', city) AS country_city
    FROM kept
    WHERE rating_adjusted <= 38
)
SELECT
    id,
    country,
    city,
    rating_adjusted,
    category,
    stock_qty,
    country_city,
    CASE WHEN city RLIKE '^(Chiang Mai|Tokyo)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS city_zone
FROM cleaned
