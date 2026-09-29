SELECT p.id, p.supplier_id, p.stock_qty
FROM products p
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.product_id IN (3, 4, 5, 7))
