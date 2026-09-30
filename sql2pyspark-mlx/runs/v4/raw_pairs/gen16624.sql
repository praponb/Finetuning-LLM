SELECT i.id, i.quantity, p.stock_qty FROM inventory i LEFT JOIN products p ON p.id = i.product_id WHERE p.brand IN ('Acme', 'Umbrella') ORDER BY p.stock_qty DESC, quantity;
