SELECT
    p.id,
    p.paid_at,
    p.status,
    o.customer_id,
    o.amount,
    o.order_date
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
WHERE p.paid_at < '2023-06-02'
