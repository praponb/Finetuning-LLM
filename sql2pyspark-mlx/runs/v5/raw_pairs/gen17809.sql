SELECT
    id,
    order_id,
    delivered_date,
    cost,
    SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS running_cost
FROM shipments
WHERE order_id IN (10, 13, 19);
