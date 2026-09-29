SELECT
    id,
    order_id,
    shipped_date,
    cost,
    LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_cost,
    MIN(cost) OVER (PARTITION BY order_id) AS order_id_min_cost
FROM shipments;
