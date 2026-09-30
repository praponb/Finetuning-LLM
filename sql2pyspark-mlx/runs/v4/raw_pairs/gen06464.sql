WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, price + cost AS margin
    FROM products
    WHERE launch_date > DATE '2023-07-12'
      AND supplier_id IS NOT NULL
      AND UPPER(TRIM(product_name)) = 'LAPTOP PRO 14'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        margin,
        ROW_NUMBER() OVER (ORDER BY margin, id) AS event_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share
    FROM filtered
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.quantity BETWEEN 109 AND 138
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        brand,
        COUNT(DISTINCT supplier_id) AS distinct_supplier_ids,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
        ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) AS cost_per_grocery,
        MIN(price) AS min_price
    FROM kept
    GROUP BY 1
    HAVING ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN category = 'Grocery' THEN 1 END), 0), 2) < 0
)
SELECT
    brand,
    cost_per_grocery,
    min_price,
    CASE WHEN brand RLIKE '^(Stark|Globex)$' THEN 'PRIORITY' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS brand_zone
FROM summary
