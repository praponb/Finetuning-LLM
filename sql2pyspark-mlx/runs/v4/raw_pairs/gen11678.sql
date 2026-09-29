SELECT
    o.customer_id,
    o.order_date,
    o.store_id,
    p.status,
    p.paid_at,
    p.method
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE NOT (p.order_id IN (1, 18, 21) AND o.amount IS NOT NULL)
ORDER BY p.paid_at, method;
