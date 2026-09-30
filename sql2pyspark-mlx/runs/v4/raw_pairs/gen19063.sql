SELECT
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE city != 'Berlin')
ORDER BY last_restock, id
LIMIT 5;
