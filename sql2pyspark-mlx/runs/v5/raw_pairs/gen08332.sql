SELECT id, customer_id, started_at FROM (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS row_num FROM sessions) AS ranked WHERE row_num <= 5
