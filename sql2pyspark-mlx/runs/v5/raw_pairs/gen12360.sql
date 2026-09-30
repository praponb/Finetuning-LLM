WITH top_sessions AS (
    SELECT
        id,
        customer_id,
        started_at,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY started_at) AS rnk
    FROM sessions
)
SELECT id, customer_id, started_at
FROM top_sessions
WHERE rnk <= 5;
