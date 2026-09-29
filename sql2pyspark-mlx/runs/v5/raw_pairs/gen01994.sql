SELECT i.product_id, i.store_id, p.cost FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id WHERE i.store_id IN (2, 6);
