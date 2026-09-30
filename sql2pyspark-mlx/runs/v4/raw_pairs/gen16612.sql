WITH ranked AS (
    SELECT
        id,
        customer_id,
        opened_date,
        account_type,
        RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) rn
    FROM accounts
)
SELECT *
FROM ranked
WHERE rn = 1
