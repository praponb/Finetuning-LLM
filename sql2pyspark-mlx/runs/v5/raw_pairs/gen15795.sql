WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN duration_sec >= 476 THEN 'High' WHEN duration_sec >= 104 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier
    FROM web_events
    WHERE page = '/home'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        new_group_flag,
        burst_id,
        ROUND(burst_id, 2) AS burst_id_rounded
    FROM sessionized
    WHERE event_time >= TIMESTAMP '2023-07-14 21:00:00'
      AND page IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY new_group_flag, id) AS row_seq,
        COUNT(duration_sec) OVER (PARTITION BY event_type, page) AS event_type_count_duration_sec,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY burst_id DESC) AS burst_id_dense_rank
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 423 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    page,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(burst_id_rounded, 0.95) AS burst_id_rounded_p95,
    SUM(event_type_count_duration_sec) FILTER (WHERE page = '/product') AS product_total_event_type_count_duration_sec,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN duration_sec >= 423 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', event_time), page), (CASE WHEN duration_sec >= 423 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', event_time)))
HAVING product_total_event_type_count_duration_sec < 1000000
