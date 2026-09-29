SELECT
    id,
    order_id,
    delivered_date,
    cost,
    LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_cost
FROM shipments;
