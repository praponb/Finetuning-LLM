SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, MIN(quantity) AS min_quantity
FROM inventory
WHERE quantity <= 229 AND store_id IN (2, 4, 5)
GROUP BY DATE_TRUNC('MONTH', last_restock)
HAVING MIN(quantity) >= 90
ORDER BY min_quantity;
