WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price <= 740
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ARRAY_CONTAINS(ch.tags, 'new')
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.cost,
        x.category
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.cost, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND LENGTH(x.product_name) >= 11
        )
)
-- Multi-level aggregation
SELECT
    country,
    CASE WHEN cost >= 600 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    ROUND(AVG(rating), 2) AS avg_rating,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS rating_per_th,
    APPROX_PERCENTILE(cost, 0.95) AS cost_p95,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    ROUND(AVG(cost), 2) AS avg_cost
FROM kept
GROUP BY GROUPING SETS ((country, CASE WHEN cost >= 600 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END), (country, CASE WHEN cost >= 600 THEN 'LARGE' ELSE 'SMALL' END), (country))
