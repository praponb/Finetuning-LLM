SELECT s.id, s.country, s.customer_id, c.credit_limit FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id;
