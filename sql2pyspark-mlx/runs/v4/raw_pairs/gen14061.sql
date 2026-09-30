SELECT
    o.status,
    o.customer_id,
    o.channel,
    p.method,
    p.status AS payment_status
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id;
