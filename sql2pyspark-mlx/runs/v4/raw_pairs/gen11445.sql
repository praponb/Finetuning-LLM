SELECT i.id, i.store_id, i.product_id, i.quantity, p.category, p.brand FROM inventory i INNER JOIN products p ON i.product_id = p.id
