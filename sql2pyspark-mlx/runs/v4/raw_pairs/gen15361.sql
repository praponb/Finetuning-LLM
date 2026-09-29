WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Multi-level aggregation
SELECT
    browser,
    customer_id,
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    COUNT(CASE WHEN browser = 'Edge' THEN 1 END) AS edge_count,
    ROUND(SUM(session_start_flag) / NULLIF(COUNT(CASE WHEN device = 'mobile' THEN 1 END), 0), 2) AS session_start_flag_per_mobile,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(AVG(session_no), 2) AS avg_session_no
FROM islands
GROUP BY browser, customer_id, CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 > 0
