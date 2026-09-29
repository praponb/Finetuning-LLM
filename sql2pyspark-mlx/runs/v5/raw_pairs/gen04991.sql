SELECT
    shipments.status,
    shipments.order_id,
    shipments.carrier,
    orders.order_date,
    orders.discount,
    orders.store_id
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
WHERE shipments.delivered_date < '2023-05-01';
