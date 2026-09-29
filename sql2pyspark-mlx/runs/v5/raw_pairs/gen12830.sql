SELECT
    id,
    order_id,
    cost,
    LENGTH(status) status_length
FROM shipments
WHERE order_id = 30
