WITH session_stats AS (
    SELECT session_id, COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    WHERE page IN ('/product', '/cart', '/home')
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    ss.home_count,
    ss.max_duration_sec
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.home_count > 7
ORDER BY ss.home_count DESC, id
LIMIT 10;
