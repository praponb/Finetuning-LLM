SELECT
    payments.id,
    payments.paid_at,
    payments.method,
    orders.channel,
    orders.status,
    stores.opened_date
FROM payments
INNER JOIN orders ON payments.order_id = orders.id
LEFT JOIN stores ON orders.store_id = stores.id
WHERE orders.discount <= 0.11;
