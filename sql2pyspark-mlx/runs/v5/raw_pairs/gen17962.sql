SELECT * FROM (SELECT category, brand, price FROM products WHERE launch_date >= '2025-04-01') PIVOT (AVG(price) FOR brand IN ('Stark', 'Umbrella')) ORDER BY category;
