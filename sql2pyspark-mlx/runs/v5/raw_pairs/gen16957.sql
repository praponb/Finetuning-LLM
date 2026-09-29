SELECT i.id, i.last_restock
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE region = 'Central');
