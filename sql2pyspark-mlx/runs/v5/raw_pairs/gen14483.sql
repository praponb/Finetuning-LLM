SELECT
    inventory.id,
    inventory.product_id,
    inventory.quantity,
    store_sales.revenue,
    store_sales.units,
    store_sales.product_id store_sale_product_id
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id;
