SELECT city FROM customers WHERE credit_limit BETWEEN 4600 AND 8400 UNION SELECT city FROM customers WHERE LOWER(name) LIKE '%any%' ORDER BY city;
