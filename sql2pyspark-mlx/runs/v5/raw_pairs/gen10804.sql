SELECT
    shipments.order_id,
    shipments.status,
    shipments.delivered_date,
    orders.amount,
    orders.channel
FROM shipments
FULL OUTER JOIN orders ON orders.id = shipments.order_id
WHERE shipments.delivered_date >= '2023-06-01'
ORDER BY order_id DESC;
