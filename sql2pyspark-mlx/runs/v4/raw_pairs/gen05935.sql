SELECT id, session_id, event_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS rnk FROM web_events) AS r WHERE rnk <= 3 ORDER BY event_time
