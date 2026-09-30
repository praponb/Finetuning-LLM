SELECT * FROM (SELECT page, session_id, event_type, duration_sec FROM web_events) src PIVOT (SUM(duration_sec) FOR event_type IN ('add_to_cart', 'purchase', 'click'))
