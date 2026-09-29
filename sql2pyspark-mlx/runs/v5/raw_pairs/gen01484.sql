WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    COUNT(CASE WHEN browser = 'Firefox' THEN 1 END) AS firefox_count,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    ROUND(SUM(session_start_flag) / NULLIF(COUNT(CASE WHEN browser = 'Edge' THEN 1 END), 0), 2) AS session_start_flag_per_edge
FROM sessions_sessions
GROUP BY DATE_TRUNC('YEAR', started_at)
ORDER BY firefox_count DESC NULLS FIRST
