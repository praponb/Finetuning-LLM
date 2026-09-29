SELECT * FROM (SELECT page, session_id, event_type, duration_sec FROM web_events) src PIVOT (MAX(duration_sec) AS total, COUNT(1) AS item_count FOR event_type IN ('click', 'purchase'))
