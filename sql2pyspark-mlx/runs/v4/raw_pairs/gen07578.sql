SELECT id, session_id, duration_sec, DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec) AS duration_sec_dense_rank FROM web_events;
