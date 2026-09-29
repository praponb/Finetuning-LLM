SELECT a.id, a.store_id, a.quantity, a.product_id FROM inventory a WHERE a.quantity < (SELECT AVG(b.quantity) FROM inventory b WHERE b.store_id = a.store_id) ORDER BY a.product_id;
