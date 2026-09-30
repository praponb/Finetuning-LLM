SELECT p.stock_qty, i.quantity
FROM products p
LEFT JOIN inventory i ON i.product_id = p.id;
