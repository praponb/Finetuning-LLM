WITH numbered AS (
    SELECT
        id,
        customer_id,
        started_at,
        device,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rnk
    FROM sessions
)
SELECT
    id,
    customer_id,
    started_at,
    device
FROM numbered
WHERE rnk <= 5
ORDER BY id DESC
