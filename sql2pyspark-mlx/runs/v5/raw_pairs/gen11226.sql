SELECT c.id, c.country, COUNT(s.id) AS session_count FROM customers c JOIN sessions s ON s.customer_id = c.id WHERE c.age >= 34 GROUP BY c.id, c.country;
