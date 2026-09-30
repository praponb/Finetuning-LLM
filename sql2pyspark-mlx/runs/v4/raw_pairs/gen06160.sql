SELECT
    s.id,
    s.delivered_date,
    o.order_date,
    c.country,
    c.signup_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE c.credit_limit <= 12300 AND LENGTH(c.name) < 11
