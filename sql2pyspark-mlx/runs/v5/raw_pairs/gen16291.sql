SELECT
    p.order_id,
    p.method,
    o.order_date,
    c.segment,
    c.is_active
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON c.id = o.customer_id;
