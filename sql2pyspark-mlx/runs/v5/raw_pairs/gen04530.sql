SELECT s.device, c.country, c.signup_date, c.city FROM sessions s INNER JOIN customers c ON c.id = s.customer_id
