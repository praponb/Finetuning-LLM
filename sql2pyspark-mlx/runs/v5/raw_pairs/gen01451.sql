WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.product_name LIKE '%Lapto%'
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_cost
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM latest_match
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE country = 'SG'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.product_name NOT LIKE '%Sho%'
        )
)
-- Aggregate with conditional metrics
SELECT
    country,
    CASE WHEN product_cost >= 740 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_cost_tier,
    COUNT(DISTINCT country) AS distinct_countries,
    ROUND(AVG(product_cost), 2) AS avg_product_cost,
    MAX(product_cost) FILTER (WHERE country = 'JP') AS jp_max_product_cost,
    SIZE(COLLECT_SET(city)) AS num_cities,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY SIZE(COLLECT_SET(city)) DESC) AS num_cities_percent_rank
FROM not_flagged
GROUP BY 1, 2
ORDER BY avg_product_cost ASC;
