SELECT
    s.shipped_date,
    s.cost,
    s.carrier,
    o.order_date,
    o.status,
    o.customer_id
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id;
