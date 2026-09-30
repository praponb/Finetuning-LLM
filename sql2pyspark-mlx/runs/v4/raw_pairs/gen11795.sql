WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS pos
    FROM accounts
)
SELECT *
FROM ranked
WHERE pos <= 3
ORDER BY id DESC;
