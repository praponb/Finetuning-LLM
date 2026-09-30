SELECT c.city, c.country, c.signup_date, s.started_at, s.browser, s.device FROM customers c INNER JOIN sessions s ON c.id = s.customer_id WHERE c.country <> 'US' OR c.age BETWEEN 55 AND 60;
