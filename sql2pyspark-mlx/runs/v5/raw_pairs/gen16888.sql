SELECT
    id,
    delivered_date,
    cost,
    status,
    shipped_date,
    LEAD(cost, 1) OVER (ORDER BY delivered_date, id) AS next_cost,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY shipped_date DESC) AS shipped_date_dense_rank
FROM shipments
