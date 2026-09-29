SELECT i.id, i.store_id, i.last_restock
FROM inventory i
WHERE i.store_id IN (SELECT id FROM stores WHERE sqft <= 8892)
ORDER BY i.last_restock;
