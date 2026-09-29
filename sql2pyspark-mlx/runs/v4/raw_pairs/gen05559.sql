SELECT id, device, started_at, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS started_at_row FROM sessions WHERE country = 'DE';
