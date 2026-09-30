SELECT
    id,
    order_id,
    shipped_date,
    cost,
    carrier,
    LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_cost,
    ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 3) AS cost_share
FROM shipments
