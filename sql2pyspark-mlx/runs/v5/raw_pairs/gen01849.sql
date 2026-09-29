SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE last_restock > '2024-12-31' AND product_id IN (9, 10)
GROUP BY product_id
HAVING stddev_quantity > 70
ORDER BY product_id;
