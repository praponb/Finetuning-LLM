SELECT id, device, started_at, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_num FROM sessions
