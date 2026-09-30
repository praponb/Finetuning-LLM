SELECT
    s.id,
    s.delivered_date,
    s.carrier,
    s.status,
    o.discount
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id;
