SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events)
PIVOT (SUM(duration_sec) FOR event_type IN ('search', 'click', 'page_view'))
ORDER BY page
