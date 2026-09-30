SELECT segment, city FROM customers INTERSECT ALL SELECT segment, city FROM customers WHERE city IS NOT NULL
