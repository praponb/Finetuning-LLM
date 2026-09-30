WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    browser,
    customer_id,
    MAX(started_at) AS last_seen,
    SUM(CASE WHEN device = 'desktop' THEN session_id ELSE 0 END) AS desktop_session_id,
    COUNT_IF(browser = 'Chrome') AS chrome_events,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows
FROM sessions_sessions
GROUP BY browser, customer_id
ORDER BY desktop_session_id NULLS FIRST;
