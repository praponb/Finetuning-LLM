SELECT
    p.id,
    p.order_id,
    p.paid_at,
    o.status,
    s.region,
    s.city
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON s.id = o.store_id
WHERE p.method NOT IN ('bank_transfer', 'card', 'promptpay')
