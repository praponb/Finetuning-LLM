WITH session_summary AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    WHERE session_id IN (2, 27)
    GROUP BY session_id
)
SELECT s.customer_id, ss.duration_sec_range, ss.avg_duration_sec
FROM sessions s
LEFT OUTER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY avg_duration_sec DESC;
