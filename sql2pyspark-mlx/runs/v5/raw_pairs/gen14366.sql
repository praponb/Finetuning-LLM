WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS row_num
    FROM accounts
    WHERE balance BETWEEN 50000 AND 63000
)
SELECT
    id,
    status,
    balance,
    account_type
FROM ranked
WHERE row_num <= 5;
