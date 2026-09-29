SELECT id, age, city FROM customers WHERE age < (SELECT MIN(age) FROM customers);
