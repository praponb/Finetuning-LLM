SELECT id, age, email FROM customers WHERE age <= (SELECT MIN(age) FROM customers);
