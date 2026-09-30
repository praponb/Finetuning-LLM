SELECT
    id,
    customer_id,
    device,
    started_at,
    DENSE_RANK() OVER (PARTITION BY customer_id, device ORDER BY started_at) AS started_at_dense_rank
FROM sessions;
