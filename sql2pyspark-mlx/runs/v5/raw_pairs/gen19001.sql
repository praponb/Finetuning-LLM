SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, store_id, COUNT(quantity) AS quantity_count
    FROM inventory
    WHERE store_id IN (2, 3)
    GROUP BY DATE_TRUNC('MONTH', last_restock), store_id
) AS x
WHERE quantity_count < 9
ORDER BY last_restock_month DESC, quantity_count DESC
