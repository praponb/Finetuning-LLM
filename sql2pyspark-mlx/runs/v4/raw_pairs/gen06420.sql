SELECT * FROM (SELECT brand, category, price FROM products WHERE launch_date > '2024-04-18')
PIVOT (AVG(price) FOR category IN ('Grocery', 'Books', 'Fashion', 'Electronics'))
ORDER BY brand;
