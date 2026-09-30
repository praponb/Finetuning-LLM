SELECT
    oi.unit_price,
    oi.quantity,
    p.price,
    p.launch_date,
    o.store_id,
    o.amount
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
WHERE p.stock_qty = 288;
