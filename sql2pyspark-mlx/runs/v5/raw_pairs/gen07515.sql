SELECT s.id, s.country, s.started_at FROM sessions s WHERE s.started_at < '2023-07-12' AND s.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@yahoo.com') ORDER BY country;
