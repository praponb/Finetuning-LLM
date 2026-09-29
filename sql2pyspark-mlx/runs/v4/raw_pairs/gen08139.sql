WITH top_sessions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS row_num FROM sessions) SELECT * FROM top_sessions WHERE row_num <= 3 ORDER BY id DESC;
