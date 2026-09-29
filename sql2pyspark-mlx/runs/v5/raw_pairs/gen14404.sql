SELECT
    oi.id,
    oi.quantity,
    oi.product_id,
    p.stock_qty,
    p.category,
    o.customer_id
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON o.id = oi.order_id
ORDER BY customer_id DESC, stock_qty DESC
