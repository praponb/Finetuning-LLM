SELECT
    last_restock,
    quantity,
    store_id,
    DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
FROM inventory
WHERE YEAR(last_restock) = 2024 AND store_id IN (2, 6)
ORDER BY last_restock, quantity DESC
