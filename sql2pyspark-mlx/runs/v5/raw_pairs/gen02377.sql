SELECT p.id, p.supplier_id, p.stock_qty
FROM products p
WHERE p.price >= 420 AND p.id IN (SELECT product_id FROM inventory)
ORDER BY 2 DESC;
