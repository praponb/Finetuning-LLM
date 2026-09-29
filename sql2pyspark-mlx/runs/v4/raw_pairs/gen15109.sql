WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) rnk
    FROM sessions
)
SELECT id, customer_id, started_at
FROM ranked
WHERE rnk <= 2
ORDER BY started_at;
