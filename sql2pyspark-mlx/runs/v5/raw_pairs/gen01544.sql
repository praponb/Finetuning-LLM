SELECT
    inventory.last_restock,
    store_sales.product_id,
    store_sales.store_id,
    store_sales.revenue
FROM inventory
JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
ORDER BY revenue
