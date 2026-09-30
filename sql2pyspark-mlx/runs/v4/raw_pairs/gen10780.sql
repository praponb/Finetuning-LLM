SELECT a.id, a.store_id, a.quantity FROM inventory a WHERE a.quantity > (SELECT AVG(b.quantity) FROM inventory b WHERE b.store_id = a.store_id) ORDER BY a.store_id
