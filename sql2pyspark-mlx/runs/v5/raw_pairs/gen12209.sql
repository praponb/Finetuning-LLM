WITH ranked AS (
    SELECT
        id,
        customer_id,
        balance,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS pos
    FROM accounts
)
SELECT
    id,
    customer_id,
    balance,
    status
FROM ranked
WHERE pos <= 5;
