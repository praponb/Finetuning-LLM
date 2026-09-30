SELECT store_id, MIN(discount) AS min_discount, ROUND(STDDEV(discount), 2) AS stddev_discount
FROM orders
GROUP BY store_id
HAVING stddev_discount > 45
