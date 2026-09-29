SELECT
    id,
    order_id,
    cost,
    shipped_date,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost DESC) AS cost_dense_rank,
    RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) AS shipped_date_rank
FROM shipments;
