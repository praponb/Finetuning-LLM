SELECT id, country, signup_date FROM customers WHERE age > 34 UNION ALL SELECT id, country, signup_date FROM customers WHERE credit_limit < 13000;
