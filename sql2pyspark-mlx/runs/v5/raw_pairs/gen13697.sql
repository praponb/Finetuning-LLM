SELECT * FROM (SELECT supplier_id, category, brand, price FROM products) src PIVOT (MAX(price) FOR brand IN ('Globex' AS globex, 'Umbrella' AS umbrella, 'Initech' AS initech))
