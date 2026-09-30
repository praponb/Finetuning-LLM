WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(carrier) AS clean_carrier,
        CAST(cost AS INT) AS cost_int
    FROM shipments
    WHERE order_id IS NOT NULL
      AND YEAR(delivered_date) >= 2025
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        cost,
        cost_int,
        LAST_VALUE(carrier, TRUE) OVER (ORDER BY cost_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, carrier), 4) AS cost_share_2
    FROM filtered
    WHERE status IN ('RETURNED', 'LOST', 'IN_TRANSIT')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM shipments
    WHERE status != 'DELIVERED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER w_recent AS cost_int_dense_rank
    FROM kept_rows
    WINDOW w_recent AS (ORDER BY cost_int NULLS LAST)
)
-- Grouped metrics
SELECT
    CASE WHEN cost_share_2 >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_share_2_tier,
    CASE WHEN cost_share >= 0.78 THEN 'HIGH' ELSE 'LOW' END AS cost_share_tier,
    PERCENTILE_APPROX(cost_share, ARRAY(0.25, 0.5, 0.75)) AS cost_share_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(cost_int_dense_rank), 2) AS avg_cost_int_dense_rank,
    PERCENTILE_APPROX(cost_share, 0.5) AS cost_share_p50,
    APPROX_PERCENTILE(cost_int, 0.5) AS cost_int_p50,
    APPROX_PERCENTILE(cost_int, ARRAY(0.25, 0.5, 0.75)) AS cost_int_quartiles
FROM enriched_shipments
GROUP BY GROUPING SETS ((CASE WHEN cost_share_2 >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN cost_share >= 0.78 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN cost_share_2 >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING PERCENTILE_APPROX(cost_share, 0.5) > 0
ORDER BY distinct_ids NULLS FIRST
