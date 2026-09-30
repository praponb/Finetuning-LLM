SELECT
    payments.id,
    payments.amount,
    payments.order_id,
    payments.paid_at,
    orders.discount,
    orders.order_date,
    orders.amount AS order_amount
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id
WHERE orders.store_id IN (19, 22)
