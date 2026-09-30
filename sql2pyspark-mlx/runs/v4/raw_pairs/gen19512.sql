SELECT i.id, i.store_id, i.last_restock
FROM inventory i
WHERE i.quantity BETWEEN 110 AND 118 AND i.store_id NOT IN (SELECT id FROM stores WHERE store_name IS NOT NULL)
ORDER BY id DESC, i.store_id
LIMIT 50;
