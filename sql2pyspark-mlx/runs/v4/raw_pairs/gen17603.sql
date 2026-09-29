SELECT i.id, i.last_restock, i.store_id, i.product_id, s.region FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id WHERE i.quantity <> 165 ORDER BY region
