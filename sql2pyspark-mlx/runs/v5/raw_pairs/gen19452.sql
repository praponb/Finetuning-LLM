SELECT id, city, age FROM customers WHERE (credit_limit <= 6500 AND city IN ('Bangkok', 'London')) OR email LIKE '%@company.co';
