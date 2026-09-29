SELECT id, age, signup_date, credit_limit FROM customers WHERE age = (SELECT MIN(age) FROM customers) ORDER BY signup_date ASC, id ASC LIMIT 1
