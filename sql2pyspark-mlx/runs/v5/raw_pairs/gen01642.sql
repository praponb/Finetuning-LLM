SELECT
    s.id,
    s.device,
    COUNT(we.id) AS web_event_count,
    MIN(we.duration_sec) AS min_duration_sec,
    ROUND((100 * SUM(CASE WHEN we.page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checkout
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
GROUP BY s.id, s.device
ORDER BY min_duration_sec DESC, device, id
LIMIT 10
