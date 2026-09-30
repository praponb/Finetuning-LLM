SELECT * FROM (SELECT TO_DATE(event_time) AS event_day, page, duration_sec FROM web_events) PIVOT (SUM(duration_sec) FOR page IN ('/checkout', '/search'));
