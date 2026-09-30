SELECT country, city, 'customer' AS source FROM customers UNION ALL SELECT country, city, 'supplier' AS source FROM suppliers WHERE supplier_name LIKE 'Gl%';
