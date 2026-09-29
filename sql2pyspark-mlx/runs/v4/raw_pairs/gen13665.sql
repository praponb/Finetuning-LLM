SELECT * FROM (SELECT category, brand, cost FROM products WHERE price IS NOT NULL) PIVOT (COUNT(cost) FOR brand IN ('Acme', 'Globex', 'Initech'));
