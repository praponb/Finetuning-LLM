SELECT
    c.name,
    c.signup_date,
    c.email,
    o.discount,
    o.customer_id,
    o.amount * o.discount AS amount_times_discount
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.amount BETWEEN 1110 AND 1150
ORDER BY discount
