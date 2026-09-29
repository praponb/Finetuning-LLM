SELECT c.id, COUNT(s.id) AS session_count, COUNT(DISTINCT s.customer_id) AS unique_customers FROM customers c LEFT JOIN sessions s ON c.id = s.customer_id GROUP BY c.id HAVING COUNT(s.id) > 1
