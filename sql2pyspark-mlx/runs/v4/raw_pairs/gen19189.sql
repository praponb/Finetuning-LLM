WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) pos
    FROM sessions
)
SELECT id, customer_id, started_at
FROM numbered
WHERE pos <= 3
ORDER BY customer_id DESC
