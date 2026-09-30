SELECT brand, category, SUM(price) AS total_price
FROM products
GROUP BY brand, category
HAVING total_price < 3300
