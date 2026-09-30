SELECT city, 'supplier' AS source FROM suppliers UNION SELECT city, 'store' AS source FROM stores ORDER BY source DESC;
