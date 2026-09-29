SELECT shipments.cost, orders.shipping_fee
FROM shipments
JOIN orders ON shipments.order_id = orders.id
WHERE shipments.order_id = 39;
