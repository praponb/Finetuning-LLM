SELECT
    inventory.product_id,
    inventory.store_id,
    inventory.quantity,
    products.supplier_id,
    products.launch_date
FROM inventory
LEFT JOIN products ON inventory.product_id = products.id
