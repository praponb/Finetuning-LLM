SELECT MONTH(last_restock) AS last_restock_month, COUNT(quantity) AS quantity_count
FROM inventory
WHERE quantity BETWEEN 124 AND 175
GROUP BY MONTH(last_restock)
HAVING COUNT(quantity) >= 4
ORDER BY last_restock_month
LIMIT 5
