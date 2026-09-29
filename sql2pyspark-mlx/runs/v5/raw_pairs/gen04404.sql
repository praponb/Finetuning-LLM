SELECT stores.id, stores.sqft
FROM stores
WHERE stores.store_name LIKE 'O%' AND NOT EXISTS (SELECT 1 FROM orders WHERE orders.store_id = stores.id)
ORDER BY stores.id DESC
