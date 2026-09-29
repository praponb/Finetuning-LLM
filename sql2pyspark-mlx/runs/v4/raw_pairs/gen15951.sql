SELECT QUARTER(last_restock) AS last_restock_quarter, store_id, MIN(last_restock) AS first_last_restock
FROM inventory
GROUP BY 1, 2
ORDER BY last_restock_quarter DESC
