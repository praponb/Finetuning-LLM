SELECT * FROM (SELECT category, brand, price FROM products)
PIVOT (MAX(price) FOR brand IN ('Umbrella', 'Stark', 'Acme', 'Initech'))
ORDER BY category;
