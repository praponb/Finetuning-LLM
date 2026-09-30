SELECT
    inventory.quantity,
    inventory.product_id,
    store_sales.units,
    store_sales.revenue
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.store_id IN (1, 2);
