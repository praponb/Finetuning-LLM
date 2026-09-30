WITH ranked_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT
    id,
    country,
    started_at,
    customer_id
FROM ranked_sessions
WHERE rnk <= 2
