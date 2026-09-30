SELECT country FROM customers UNION SELECT country FROM sessions WHERE customer_id IN (9, 25) ORDER BY country
