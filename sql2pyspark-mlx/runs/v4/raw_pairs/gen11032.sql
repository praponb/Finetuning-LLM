SELECT
    s.id,
    s.status,
    o.store_id,
    o.discount,
    c.segment,
    c.credit_limit
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
LEFT JOIN customers c ON c.id = o.customer_id;
