SELECT
    inventory.last_restock,
    stores.region,
    stores.opened_date,
    products.launch_date
FROM inventory
LEFT OUTER JOIN stores ON stores.id = inventory.store_id
LEFT OUTER JOIN products ON inventory.product_id = products.id;
