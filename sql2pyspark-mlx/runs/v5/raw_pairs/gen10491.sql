SELECT
    id,
    customer_id,
    started_at,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS started_at_dense_rank
FROM sessions;
