SELECT
    shipments.order_id,
    orders.status,
    orders.channel,
    customers.segment
FROM shipments
LEFT JOIN orders ON shipments.order_id = orders.id
JOIN customers ON customers.id = orders.customer_id
WHERE shipments.status <> 'IN_TRANSIT' AND customers.city <> 'London'
ORDER BY customers.segment ASC, order_id DESC
