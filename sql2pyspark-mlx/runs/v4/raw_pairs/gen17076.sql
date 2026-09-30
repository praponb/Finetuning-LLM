SELECT
    p.order_id,
    p.paid_at,
    o.amount,
    o.store_id,
    o.shipping_fee
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
WHERE p.order_id IS NOT NULL
ORDER BY order_id
