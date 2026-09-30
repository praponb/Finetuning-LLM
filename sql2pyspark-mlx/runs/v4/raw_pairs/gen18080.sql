SELECT id, duration_sec, DENSE_RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_dense_rank FROM web_events
