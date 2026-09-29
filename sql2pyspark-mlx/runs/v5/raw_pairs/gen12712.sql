SELECT c.email, c.city, s.device, s.started_at FROM customers c LEFT JOIN sessions s ON s.customer_id = c.id WHERE s.browser IN ('Safari', 'Edge') ORDER BY city
