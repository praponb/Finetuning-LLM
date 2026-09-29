SELECT oi.quantity, p.category, p.stock_qty
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE ARRAY_CONTAINS(p.tags, 'new') AND oi.product_id IN (3, 13, 36)
ORDER BY stock_qty DESC
