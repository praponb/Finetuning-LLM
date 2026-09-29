WITH session_stats AS (
    SELECT session_id, COUNT(duration_sec) AS duration_sec_count, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.customer_id,
    ss.duration_sec_count,
    ss.avg_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
ORDER BY s.country DESC, id
LIMIT 5
