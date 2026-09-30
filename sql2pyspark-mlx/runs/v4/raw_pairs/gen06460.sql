WITH ranked_accounts AS (
    SELECT
        id,
        status,
        opened_date,
        customer_id,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY opened_date) rnk
    FROM accounts
)
SELECT
    id,
    status,
    opened_date,
    customer_id
FROM ranked_accounts
WHERE rnk = 1
