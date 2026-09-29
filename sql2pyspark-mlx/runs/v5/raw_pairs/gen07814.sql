SELECT payments.amount, orders.order_date
FROM payments
LEFT JOIN orders ON payments.order_id = orders.id
WHERE orders.shipping_fee < 38 AND payments.amount > 1020
ORDER BY order_date
