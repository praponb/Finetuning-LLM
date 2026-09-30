SELECT city, 'supplier' AS source FROM suppliers UNION ALL SELECT city, 'store' AS source FROM stores WHERE city = 'Chiang Mai';
