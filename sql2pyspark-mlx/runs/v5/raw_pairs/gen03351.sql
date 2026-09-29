SELECT
    s.browser,
    s.device,
    SUM(CASE WHEN we.page = '/home' THEN 1 ELSE 0 END) AS home_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.event_type))) AS event_type_values
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
GROUP BY s.browser, s.device
HAVING SUM(CASE WHEN we.page = '/home' THEN 1 ELSE 0 END) >= 33
ORDER BY browser DESC;
