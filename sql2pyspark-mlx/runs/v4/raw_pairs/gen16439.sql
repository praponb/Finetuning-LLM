SELECT
    oi.id,
    oi.quantity,
    o.status,
    o.order_date,
    p.stock_qty,
    p.cost
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN products p ON oi.product_id = p.id
ORDER BY o.status;
