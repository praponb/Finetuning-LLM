SELECT s.country, SUM(CASE WHEN we.event_type = 'search' THEN 1 ELSE 0 END) AS search_count, MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
GROUP BY s.country
