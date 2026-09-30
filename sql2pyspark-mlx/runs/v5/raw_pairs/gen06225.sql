SELECT id, device, started_at FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at DESC) AS row_num FROM sessions WHERE device = 'tablet') AS ranked WHERE row_num <= 3;
