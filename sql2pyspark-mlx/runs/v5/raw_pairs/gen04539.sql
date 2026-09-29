SELECT * FROM (SELECT QUARTER(event_time) AS event_quarter, page, duration_sec FROM web_events)
PIVOT (MAX(duration_sec) FOR page IN ('/search', '/home', '/checkout'))
