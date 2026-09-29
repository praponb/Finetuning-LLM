SELECT category, ROUND(STDDEV(price), 2) AS stddev_price
FROM products
GROUP BY 1
HAVING ROUND(STDDEV(price), 2) > 73
ORDER BY category;
