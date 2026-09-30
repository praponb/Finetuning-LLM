SELECT
    c.segment,
    c.signup_date,
    o.shipping_fee,
    o.discount,
    o.status
FROM customers c
JOIN orders o ON o.customer_id = c.id
WHERE c.city = 'Bangkok' AND o.order_date > '2023-09-01'
ORDER BY status
