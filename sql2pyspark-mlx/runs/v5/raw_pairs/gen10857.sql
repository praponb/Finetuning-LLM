SELECT s.browser, c.email, c.city, c.country FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id WHERE c.credit_limit >= 15500 ORDER BY country;
