SELECT location, 'department' AS origin FROM departments WHERE location = 'London' UNION ALL SELECT city, 'customer' AS origin FROM customers;
