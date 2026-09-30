SELECT s.device, c.name FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.city IS NOT NULL ORDER BY s.device;
