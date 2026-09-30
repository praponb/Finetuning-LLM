WITH top_sessions AS (
    SELECT
        id,
        customer_id,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rnk
    FROM sessions
    WHERE device IN ('mobile', 'tablet')
)
SELECT *
FROM top_sessions
WHERE rnk <= 3
