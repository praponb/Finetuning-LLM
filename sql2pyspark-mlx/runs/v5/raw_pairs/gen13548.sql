SELECT 'customer' AS source, city, country FROM customers WHERE city IN ('New York', 'London') UNION ALL SELECT 'supplier' AS source, city, country FROM suppliers
