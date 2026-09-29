SELECT * FROM (SELECT supplier_id, category, brand, price FROM products) src PIVOT (AVG(price) AS value_sum, COUNT(1) AS n FOR brand IN ('Stark', 'Initech'));
