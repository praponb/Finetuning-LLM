SELECT c.is_active, MIN(s.started_at) first_started_at FROM sessions s INNER JOIN customers c ON s.customer_id = c.id GROUP BY c.is_active ORDER BY first_started_at;
