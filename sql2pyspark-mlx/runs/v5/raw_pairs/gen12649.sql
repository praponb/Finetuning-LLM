SELECT id, country, started_at FROM sessions WHERE customer_id IN (11, 15, 33, 34) AND browser IS NOT NULL ORDER BY started_at, id LIMIT 1;
