WITH Ranked AS (
    SELECT
        id,
        customer_id,
        started_at,
        country,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) pos
    FROM sessions
    WHERE customer_id = 15
)
SELECT
    id,
    customer_id,
    started_at,
    country
FROM Ranked
WHERE pos <= 3;
