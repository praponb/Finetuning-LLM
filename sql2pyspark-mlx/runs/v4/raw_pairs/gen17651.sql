SELECT i.id, i.store_id, i.quantity FROM inventory i WHERE i.quantity > (SELECT AVG(i2.quantity) FROM inventory i2 WHERE i2.store_id = i.store_id) ORDER BY i.quantity DESC;
