SELECT i.id, i.product_id, i.store_id
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE city = 'Singapore')
