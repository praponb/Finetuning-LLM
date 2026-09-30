SELECT product_id, MAX(quantity) AS max_quantity, MAX(last_restock) AS last_last_restock
FROM inventory
WHERE quantity = 210 OR last_restock > '2024-08-01'
GROUP BY 1
ORDER BY product_id DESC, 3
