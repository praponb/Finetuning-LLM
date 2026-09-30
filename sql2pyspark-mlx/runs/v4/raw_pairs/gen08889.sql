WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Grouped metrics
SELECT
    event_type,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS duration_sec_per_search,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    ROUND(AVG(session_no), 2) AS avg_session_no
FROM sessions_web_events
GROUP BY 1
HAVING avg_session_no >= 0
