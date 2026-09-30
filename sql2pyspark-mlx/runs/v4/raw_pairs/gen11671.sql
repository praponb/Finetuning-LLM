SELECT
    s.status,
    s.cost,
    o.customer_id,
    c.signup_date,
    o.discount - o.shipping_fee AS discount_minus_shipping_fee
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN customers c ON c.id = o.customer_id
WHERE s.shipped_date < '2025-04-05';
