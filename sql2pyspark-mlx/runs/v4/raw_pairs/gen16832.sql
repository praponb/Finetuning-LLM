SELECT
    c.is_active,
    o.amount,
    o.status,
    o.shipping_fee
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id
WHERE o.store_id IN (1, 10, 28)
ORDER BY shipping_fee DESC
