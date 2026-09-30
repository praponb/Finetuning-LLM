SELECT
    orders.id,
    orders.customer_id,
    orders.amount,
    orders.channel
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id AND shipments.delivered_date >= '2024-04-23')
