SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY device ORDER BY started_at) AS row_num FROM sessions) AS ranked WHERE row_num <= 2 ORDER BY customer_id DESC;
