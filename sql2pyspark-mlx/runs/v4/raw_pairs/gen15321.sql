WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date < '2024-04-01'
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY rating NULLS LAST) AS rating_cume_dist
    FROM eligible
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.stock_qty
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    country,
    APPROX_PERCENTILE(rating_cume_dist, 0.5) AS rating_cume_dist_p50,
    COUNT_IF(country = 'SG') AS sg_events,
    SUM(CASE WHEN city = 'Tokyo' THEN stock_qty ELSE 0 END) AS tokyo_stock_qty
FROM latest_match
GROUP BY country
