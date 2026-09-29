SELECT
    o.discount,
    o.shipping_fee,
    o.amount,
    p.order_id,
    p.status,
    p.paid_at
FROM orders o
JOIN payments p ON o.id = p.order_id
WHERE p.amount BETWEEN 990 AND 1450 AND o.status = 'COMPLETED'
