WITH session_stats AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.customer_id, ss.stddev_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
ORDER BY customer_id;
