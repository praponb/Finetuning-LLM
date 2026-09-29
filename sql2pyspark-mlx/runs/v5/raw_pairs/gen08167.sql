WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_num FROM sessions) SELECT id, device, started_at, country FROM Numbered WHERE row_num <= 1;
