WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN device IN ('tablet', 'desktop') THEN 'Group A' ELSE 'Other' END AS device_group
    FROM sessions
    WHERE browser IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        device_group,
        SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    burst_id,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    ROUND(AVG(burst_id), 2) AS avg_burst_id,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    MAX(started_at) AS last_started_at,
    MAX(burst_id) FILTER (WHERE device = 'tablet') AS tablet_max_burst_id
FROM sessionized
GROUP BY GROUPING SETS ((burst_id, DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('MONTH', started_at)), (burst_id, DATE_TRUNC('QUARTER', started_at)));
