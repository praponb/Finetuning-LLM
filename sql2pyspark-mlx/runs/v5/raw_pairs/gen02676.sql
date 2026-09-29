WITH remaining_keys AS (
    SELECT id
    FROM shipments
    EXCEPT ALL
    SELECT id
    FROM shipments
    WHERE order_id IS NULL
),
verified AS (
    SELECT s.*
    FROM shipments s
    JOIN remaining_keys k ON s.id = k.id
)
SELECT
    order_id,
    DATE_TRUNC('WEEK', delivered_date) AS week_start,
    AVG(cost) AS avg_cost,
    MIN(cost) AS min_cost,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END), 0), 2) AS cost_per_kerry,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    SUM(CASE WHEN carrier = 'UPS' THEN cost ELSE 0 END) AS ups_cost,
    SIZE(COLLECT_SET(status)) AS num_status,
    GROUPING_ID() AS grouping_level
FROM verified
GROUP BY GROUPING SETS ((order_id, DATE_TRUNC('WEEK', delivered_date)), (order_id))
ORDER BY min_cost;
