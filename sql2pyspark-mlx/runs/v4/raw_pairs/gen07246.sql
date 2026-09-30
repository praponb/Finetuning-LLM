SELECT city, 'supplier' AS source FROM suppliers WHERE rating >= 1.9 UNION SELECT city, 'store' AS source FROM stores ORDER BY city;
