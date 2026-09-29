SELECT
    inventory.id,
    inventory.last_restock,
    inventory.store_id,
    products.launch_date,
    products.product_name,
    stores.sqft
FROM inventory
INNER JOIN products ON products.id = inventory.product_id
LEFT JOIN stores ON stores.id = inventory.store_id
WHERE products.category = 'Books';
