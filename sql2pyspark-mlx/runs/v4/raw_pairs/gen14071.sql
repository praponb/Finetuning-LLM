SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
WHERE product_id IS NULL AND unit_price >= 310
GROUP BY 1
HAVING ROUND(STDDEV(unit_price), 2) > 35
ORDER BY product_id DESC
LIMIT 20;
