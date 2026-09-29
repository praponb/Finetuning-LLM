WITH session_stats AS (
    SELECT session_id, COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.browser, ss.add_to_cart_count
FROM sessions s
LEFT JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.add_to_cart_count > 6
ORDER BY add_to_cart_count DESC
