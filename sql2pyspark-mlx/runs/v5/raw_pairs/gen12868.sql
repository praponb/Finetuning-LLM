SELECT id, customer_id, started_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rn
    FROM sessions
) AS ranked
WHERE rn <= 5
ORDER BY started_at ASC
