SELECT stores.id, stores.city
FROM stores
WHERE NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id)
ORDER BY city, stores.id DESC
LIMIT 5
