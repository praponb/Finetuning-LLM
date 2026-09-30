SELECT p.id, p.stock_qty
FROM products p
WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.product_id IN (1, 6, 7))
