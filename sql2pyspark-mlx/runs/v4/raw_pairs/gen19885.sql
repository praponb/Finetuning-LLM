SELECT s.browser, MIN(we.duration_sec) AS min_duration_sec, ROUND((100 * SUM(CASE WHEN we.event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_page_view
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
GROUP BY s.browser
ORDER BY 3, 2 DESC;
