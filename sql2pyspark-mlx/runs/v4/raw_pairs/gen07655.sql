SELECT oi.unit_price, p.stock_qty, o.order_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
LEFT OUTER JOIN orders o ON oi.order_id = o.id
WHERE o.status != 'PENDING'
ORDER BY stock_qty DESC;
