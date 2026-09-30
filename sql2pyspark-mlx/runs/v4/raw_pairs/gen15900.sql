SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE session_id IN (5, 21, 26))
PIVOT (AVG(duration_sec) FOR event_type IN ('page_view', 'purchase', 'click'))
ORDER BY page
