WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS row_num FROM web_events) SELECT id, session_id, duration_sec FROM numbered WHERE row_num = 1;
