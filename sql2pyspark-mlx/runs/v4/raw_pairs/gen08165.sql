SELECT
    oi.id,
    oi.order_id,
    oi.product_id,
    p.cost,
    p.stock_qty
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
WHERE p.price >= 840 AND p.stock_qty < 263;
