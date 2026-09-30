WITH ranked AS (SELECT id, session_id, duration_sec, page, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) rn FROM web_events) SELECT * FROM ranked WHERE rn = 1
