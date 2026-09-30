SELECT page, row_count FROM (SELECT page, COUNT(*) AS row_count FROM web_events WHERE event_type = 'click' GROUP BY page) AS x WHERE row_count < 5
