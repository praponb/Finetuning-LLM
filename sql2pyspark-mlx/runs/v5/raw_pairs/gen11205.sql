SELECT c.id, c.segment, COUNT(s.id) AS session_count, MAX(s.started_at) AS last_started_at FROM customers c INNER JOIN sessions s ON s.customer_id = c.id GROUP BY c.id, c.segment;
