SELECT inventory.product_id, stores.store_name, stores.region
FROM inventory
JOIN stores ON inventory.store_id = stores.id
WHERE stores.opened_date <= '2025-05-01' AND stores.region = 'East';
