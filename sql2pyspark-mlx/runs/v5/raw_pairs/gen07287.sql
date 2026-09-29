WITH ranked_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_num
    FROM sessions
)
SELECT
    id,
    country,
    started_at,
    customer_id
FROM ranked_sessions
WHERE row_num <= 5
ORDER BY customer_id DESC, id
