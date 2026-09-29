SELECT * FROM (SELECT category, brand, price FROM products WHERE category = 'Fashion')
PIVOT (MAX(price) FOR brand IN ('Stark', 'Umbrella', 'Initech', 'Globex'))
ORDER BY category;
