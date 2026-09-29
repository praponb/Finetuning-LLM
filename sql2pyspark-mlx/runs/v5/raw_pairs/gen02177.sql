SELECT
    s.id,
    s.delivered_date,
    s.carrier,
    o.store_id,
    o.order_date,
    c.country,
    c.credit_limit
FROM shipments s
RIGHT OUTER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN customers c ON c.id = o.customer_id;
