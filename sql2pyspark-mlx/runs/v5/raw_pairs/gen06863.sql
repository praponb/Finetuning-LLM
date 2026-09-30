SELECT
    oi.quantity,
    oi.product_id,
    o.order_date,
    o.shipping_fee,
    p.launch_date,
    p.stock_qty
FROM order_items oi
FULL OUTER JOIN orders o ON o.id = oi.order_id
FULL OUTER JOIN products p ON oi.product_id = p.id
ORDER BY oi.product_id ASC;
