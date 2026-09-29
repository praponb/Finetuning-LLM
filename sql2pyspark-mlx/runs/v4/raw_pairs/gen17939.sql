SELECT id, order_date, COALESCE(channel, 'unassigned') AS channel_filled
FROM orders
WHERE YEAR(order_date) = 2023
