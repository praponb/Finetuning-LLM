SELECT i.id, i.product_id
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE stock_qty <= 241);
