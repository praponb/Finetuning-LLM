SELECT
    id,
    delivered_date,
    cost,
    order_id
FROM shipments
WHERE delivered_date >= (SELECT MAX(delivered_date) FROM shipments);
