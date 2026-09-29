SELECT id, customer_id, started_at, device FROM sessions WHERE HOUR(started_at) BETWEEN 6 AND 13 AND customer_id IN (3, 21, 28, 38)
