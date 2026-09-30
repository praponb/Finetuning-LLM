SELECT
    o.order_date,
    o.shipping_fee,
    o.channel,
    p.order_id,
    p.status,
    p.paid_at
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id;
