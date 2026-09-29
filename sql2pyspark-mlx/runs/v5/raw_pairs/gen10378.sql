SELECT * FROM (SELECT category, brand, price FROM products) PIVOT (AVG(price) FOR brand IN ('Globex', 'Initech')) ORDER BY category
