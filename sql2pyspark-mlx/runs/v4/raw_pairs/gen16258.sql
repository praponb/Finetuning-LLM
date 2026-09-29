SELECT brand, ROUND(STDDEV(price), 2) AS stddev_price, SUM(CASE WHEN brand = 'Globex' THEN cost ELSE 0 END) AS globex_cost
FROM products
WHERE UPPER(product_name) LIKE '%IRE%'
GROUP BY 1;
