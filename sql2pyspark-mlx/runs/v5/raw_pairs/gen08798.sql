SELECT category, brand, ROUND(AVG(price), 2) AS avg_price
FROM products
GROUP BY category, brand
ORDER BY avg_price DESC, category ASC
