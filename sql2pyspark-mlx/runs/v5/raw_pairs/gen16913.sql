SELECT
    inventory.id,
    inventory.product_id,
    inventory.quantity,
    inventory.last_restock,
    store_sales.sale_date,
    store_sales.revenue
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.sale_date >= '2023-07-27' AND store_sales.store_id IN (2, 4, 5)
ORDER BY inventory.product_id;
