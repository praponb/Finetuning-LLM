SELECT * FROM customers WHERE (credit_limit <= 12300 AND UPPER(TRIM(name)) = 'BOB SMITH') OR age >= 67
