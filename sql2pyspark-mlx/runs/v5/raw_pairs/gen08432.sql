SELECT product_id, store_id, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
FROM inventory
WHERE store_id IN (2, 3, 5)
