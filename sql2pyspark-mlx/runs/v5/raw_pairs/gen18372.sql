SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events WHERE page = '/cart')
PIVOT (SUM(duration_sec) FOR event_type IN ('page_view', 'purchase'))
