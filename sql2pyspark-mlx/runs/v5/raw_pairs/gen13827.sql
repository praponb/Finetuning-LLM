SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS LAST, id) AS row_num FROM sessions) x WHERE row_num = 1
