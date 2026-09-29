SELECT
    s.status,
    s.shipped_date,
    s.carrier,
    o.customer_id,
    o.amount
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
ORDER BY customer_id DESC
