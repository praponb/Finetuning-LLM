SELECT
    id,
    order_id,
    cost,
    carrier
FROM shipments
WHERE YEAR(delivered_date) = 2025
