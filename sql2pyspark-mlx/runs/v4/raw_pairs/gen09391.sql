SELECT
    id,
    started_at,
    device,
    ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS rn,
    DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at) AS started_at_dense_rank
FROM sessions
WHERE customer_id = 27;
