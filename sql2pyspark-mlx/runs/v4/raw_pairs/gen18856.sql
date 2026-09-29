SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_num FROM sessions) AS r WHERE row_num <= 5;
