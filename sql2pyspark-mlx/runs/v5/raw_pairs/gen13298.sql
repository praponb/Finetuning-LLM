SELECT MONTH(last_restock) AS last_restock_month, AVG(quantity) AS avg_quantity, MIN(quantity) AS min_quantity
FROM inventory
WHERE last_restock > '2023-11-01' AND product_id IN (9, 10)
GROUP BY 1
ORDER BY min_quantity DESC, avg_quantity DESC
