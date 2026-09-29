SELECT s.id, s.country, c.age FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.segment != 'SMB' AND s.customer_id IS NOT NULL ORDER BY age DESC, id LIMIT 5;
