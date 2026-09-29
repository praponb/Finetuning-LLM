SELECT city FROM customers WHERE email NOT LIKE '%@yahoo.com' UNION ALL SELECT city FROM stores WHERE city IN ('London', 'Berlin', 'Chiang Mai') ORDER BY city;
