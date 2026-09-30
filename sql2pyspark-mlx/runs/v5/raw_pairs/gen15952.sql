SELECT
    shipments.id,
    shipments.cost,
    shipments.status,
    shipments.order_id
FROM shipments
WHERE shipments.order_id IN (SELECT id FROM orders WHERE amount < 420)
