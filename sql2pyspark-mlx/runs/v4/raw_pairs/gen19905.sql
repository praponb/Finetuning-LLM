SELECT category, ROUND(STDDEV(price), 2) AS stddev_price
FROM products
WHERE price < 1150 AND LENGTH(product_name) > 8
GROUP BY category
HAVING ROUND(STDDEV(price), 2) < 23
ORDER BY category ASC;
