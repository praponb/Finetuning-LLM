WITH ranked AS (
    SELECT
        id,
        device,
        started_at,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    customer_id
FROM ranked
WHERE rnk <= 3
ORDER BY id, started_at;
