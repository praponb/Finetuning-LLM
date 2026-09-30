SELECT
    o.amount,
    c.segment,
    c.signup_date,
    s.region,
    s.opened_date
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
WHERE o.order_date >= '2023-11-01'
