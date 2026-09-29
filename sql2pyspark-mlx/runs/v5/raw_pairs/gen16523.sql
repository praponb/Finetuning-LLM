SELECT city, country, 'customer' AS source FROM customers UNION SELECT city, country, 'supplier' AS source FROM suppliers
