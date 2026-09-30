SELECT
    payments.order_id,
    orders.discount,
    orders.store_id,
    orders.customer_id
FROM payments
JOIN orders ON payments.order_id = orders.id
WHERE orders.shipping_fee <= 37 OR payments.amount >= 550
