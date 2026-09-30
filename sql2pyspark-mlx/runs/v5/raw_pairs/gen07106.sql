SELECT age FROM customers WHERE is_active = FALSE UNION SELECT age FROM customers WHERE signup_date >= '2024-12-01' ORDER BY age;
