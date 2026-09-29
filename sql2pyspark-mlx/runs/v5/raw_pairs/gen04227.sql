SELECT
    s.cost,
    s.carrier,
    o.channel,
    o.customer_id,
    o.order_date
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
ORDER BY customer_id DESC;
