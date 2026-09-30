SELECT * FROM (SELECT supplier_id, brand, price FROM products) src PIVOT (SUM(price) AS amount FOR brand IN ('Acme', 'Globex'));
