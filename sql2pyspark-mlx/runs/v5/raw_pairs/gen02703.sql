WITH with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_stock_qty
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        ROUND(product_stock_qty * 1.15, 2) AS product_stock_qty_adjusted,
        LOWER(country) AS country_lower,
        COALESCE(city, 'N/A') AS city_filled
    FROM with_latest
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Tokyo'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date > '2023-08-01'
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_cost
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand IN ('Initech', 'Stark')
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(rating) OVER (PARTITION BY city ORDER BY product_stock_qty_adjusted, id) AS prev_rating,
        LEAD(rating) OVER (PARTITION BY city ORDER BY product_stock_qty_adjusted, id) AS next_rating,
        LAG(product_cost) OVER (PARTITION BY city ORDER BY product_stock_qty_adjusted, id) AS prev_product_cost,
        LEAD(product_cost) OVER (PARTITION BY city ORDER BY product_stock_qty_adjusted, id) AS next_product_cost,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY product_stock_qty_adjusted, id) AS event_seq
    FROM not_flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY product_cost NULLS LAST) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
)
SELECT
    id,
    city,
    product_stock_qty_adjusted,
    country_lower,
    product_cost,
    prev_rating,
    next_rating,
    next_product_cost,
    event_seq,
    CASE WHEN city RLIKE '^(Berlin|Tokyo)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM deduped;
