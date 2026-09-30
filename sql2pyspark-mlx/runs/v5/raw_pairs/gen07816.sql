SELECT * FROM (SELECT category, brand, cost FROM products WHERE brand = 'Initech')
PIVOT (COUNT(cost) FOR brand IN ('Stark', 'Acme'))
ORDER BY category;
