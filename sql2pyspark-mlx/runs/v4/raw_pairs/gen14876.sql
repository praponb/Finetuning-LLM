SELECT
    s.id,
    s.delivered_date,
    o.order_date,
    o.channel,
    c.credit_limit,
    c.segment
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
