SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(quantity) AS max_quantity
FROM inventory
WHERE last_restock < '2024-09-19' AND last_restock > '2025-01-26'
GROUP BY product_id
HAVING MAX(quantity) < 210
ORDER BY product_id;
