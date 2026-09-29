SELECT city, 'customer' AS source FROM customers UNION ALL SELECT city, 'store' AS source FROM stores;
