SELECT i.quantity, p.supplier_id, p.price, p.category FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE p.cost IS NOT NULL
