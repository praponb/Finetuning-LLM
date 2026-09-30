SELECT channel, MIN(order_date) AS first_order_date, MAX(order_date) AS latest_order_date
FROM orders
WHERE shipping_fee < 14
GROUP BY channel
ORDER BY first_order_date DESC
