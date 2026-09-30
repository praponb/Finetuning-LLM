SELECT
    s.carrier,
    s.order_id,
    o.amount,
    o.status,
    c.name,
    c.signup_date
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
FULL OUTER JOIN customers c ON o.customer_id = c.id
ORDER BY 6 DESC;
