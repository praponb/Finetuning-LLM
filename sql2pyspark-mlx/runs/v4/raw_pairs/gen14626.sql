SELECT
    stores.opened_date,
    stores.city,
    stores.region,
    inventory.store_id
FROM stores
INNER JOIN inventory ON inventory.store_id = stores.id
WHERE stores.store_name IS NOT NULL AND inventory.last_restock > '2024-11-01'
ORDER BY inventory.store_id;
