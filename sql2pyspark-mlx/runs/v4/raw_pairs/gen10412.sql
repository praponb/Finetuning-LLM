SELECT s.device, s.country, t.pct_click
FROM sessions s
JOIN (SELECT session_id, ROUND((100 * SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_click
    FROM web_events
    WHERE session_id IN (4, 24, 27, 38)
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY s.device DESC
