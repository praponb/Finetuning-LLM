SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
GROUP BY product_id
