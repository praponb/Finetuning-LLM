SELECT i.id, i.quantity, i.store_id FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 7897 AND 9959) ORDER BY i.store_id DESC;
