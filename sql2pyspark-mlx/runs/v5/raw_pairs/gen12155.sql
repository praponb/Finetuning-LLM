SELECT
    id,
    order_id,
    shipped_date,
    cost,
    SUM(cost) OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM shipments;
