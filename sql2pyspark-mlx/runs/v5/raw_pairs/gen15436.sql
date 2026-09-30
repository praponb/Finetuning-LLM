SELECT i.id, i.last_restock, i.quantity
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE region IN ('East', 'Central', 'West'));
