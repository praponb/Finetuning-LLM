WITH enriched AS (
    SELECT
        *,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cost,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY cost DESC, id) cost_bucket
    FROM shipments
)
SELECT
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    status,
    ROUND(SUM(cost_bucket) / NULLIF(COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END), 0), 2) AS cost_bucket_per_in_transit,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SUM(cost) AS total_cost
FROM enriched
GROUP BY ROLLUP (DATE_TRUNC('MONTH', delivered_date), status)
HAVING cost_bucket_per_in_transit < 0
ORDER BY total_cost DESC;
