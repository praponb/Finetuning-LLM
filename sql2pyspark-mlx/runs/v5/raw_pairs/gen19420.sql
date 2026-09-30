SELECT
    o.shipping_fee,
    o.amount,
    p.paid_at,
    p.status,
    p.method
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE p.method = 'card' OR p.method IN ('promptpay', 'bank_transfer')
ORDER BY amount DESC
