SELECT country, 'customer' AS source FROM customers UNION SELECT country, 'session' AS source FROM sessions WHERE started_at >= '2025-05-01' ORDER BY country DESC;
