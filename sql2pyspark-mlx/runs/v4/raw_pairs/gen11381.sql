SELECT channel, MIN(order_date) first_order_date
FROM orders
GROUP BY channel
