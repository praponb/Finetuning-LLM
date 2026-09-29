SELECT c.city, COUNT(s.id) AS session_count, COUNT(DISTINCT s.country) AS unique_countrys FROM customers c LEFT JOIN sessions s ON s.customer_id = c.id GROUP BY c.city
