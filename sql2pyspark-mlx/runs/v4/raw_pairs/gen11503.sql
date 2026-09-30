SELECT * FROM (SELECT supplier_id, brand, price FROM products WHERE brand = 'Globex') PIVOT (SUM(price) FOR brand IN ('Initech', 'Umbrella', 'Stark')) ORDER BY supplier_id
