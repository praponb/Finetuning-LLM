SELECT id, customer_id, started_at, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rn FROM sessions ORDER BY started_at
