SELECT
    s.id,
    s.browser,
    COUNT(we.id) AS web_event_count,
    COUNT(we.duration_sec) AS duration_sec_count,
    AVG(we.duration_sec) AS avg_duration_sec
FROM sessions s
LEFT OUTER JOIN web_events we ON s.id = we.session_id
WHERE TO_DATE(s.started_at) < '2025-01-28'
GROUP BY s.id, s.browser
ORDER BY s.browser, s.id;
