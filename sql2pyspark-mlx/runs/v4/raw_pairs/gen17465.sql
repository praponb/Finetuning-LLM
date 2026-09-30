SELECT id, discount, order_date
FROM orders
WHERE shipping_fee <= 38 AND customer_id IS NULL AND discount <= 0.3
