WITH Ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS pos FROM web_events) SELECT id, event_type, event_time, session_id FROM Ranked WHERE pos <= 3
