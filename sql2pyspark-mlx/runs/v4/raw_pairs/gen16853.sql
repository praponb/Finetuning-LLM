SELECT
    id,
    order_id,
    cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 3) AS cost_share
FROM shipments;
