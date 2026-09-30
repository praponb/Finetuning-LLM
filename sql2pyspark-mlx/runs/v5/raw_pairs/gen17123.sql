SELECT
    o.shipping_fee,
    o.status,
    o.channel,
    p.paid_at,
    p.order_id
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE p.paid_at >= '2023-06-20';
