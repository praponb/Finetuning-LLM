SELECT
    s.id,
    s.browser,
    COUNT(we.id) AS web_event_count,
    ROUND((100 * SUM(CASE WHEN we.page = '/product' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_product,
    SUM(CASE WHEN we.page = '/home' THEN we.duration_sec ELSE 0 END) AS home_duration_sec
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.device IN ('desktop', 'tablet', 'mobile')
GROUP BY s.id, s.browser
ORDER BY home_duration_sec DESC, id DESC, browser ASC
LIMIT 1;
