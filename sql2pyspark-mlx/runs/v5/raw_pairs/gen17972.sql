SELECT id, age, name FROM customers WHERE name LIKE '%Kim%' UNION SELECT id, age, name FROM customers WHERE city = 'Chiang Mai';
