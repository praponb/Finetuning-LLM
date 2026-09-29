SELECT
    id,
    order_id,
    shipped_date,
    cost,
    status,
    LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_cost,
    MAX(cost) OVER (PARTITION BY status) AS status_max_cost
FROM shipments
ORDER BY id
