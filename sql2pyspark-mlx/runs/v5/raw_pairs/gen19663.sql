SELECT id, session_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY event_time NULLS FIRST, id) AS dedup_rank FROM web_events) x WHERE dedup_rank = 1
