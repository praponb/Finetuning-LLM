SELECT
    oi.id,
    oi.order_id,
    oi.quantity,
    o.shipping_fee,
    p.price,
    p.brand
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
RIGHT JOIN products p ON p.id = oi.product_id;
