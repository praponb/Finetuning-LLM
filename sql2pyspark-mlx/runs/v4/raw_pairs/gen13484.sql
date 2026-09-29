SELECT s.country, s.browser, c.email, c.name, c.city FROM sessions s INNER JOIN customers c ON s.customer_id = c.id ORDER BY 4;
