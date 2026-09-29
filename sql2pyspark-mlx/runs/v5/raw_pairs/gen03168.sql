SELECT
    p.id,
    p.product_name,
    p.stock_qty,
    p.cost
FROM products p
WHERE p.cost >= 560 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.product_id IN (2, 4))
ORDER BY p.product_name DESC, stock_qty DESC
