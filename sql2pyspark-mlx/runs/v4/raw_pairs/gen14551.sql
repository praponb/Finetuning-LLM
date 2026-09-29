SELECT
    store_sales.store_id,
    store_sales.product_id,
    AVG(inventory.quantity) AS avg_quantity,
    COUNT(DISTINCT inventory.quantity) AS unique_stores
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.store_id IN (5, 6)
GROUP BY store_sales.store_id, store_sales.product_id
ORDER BY store_sales.store_id
