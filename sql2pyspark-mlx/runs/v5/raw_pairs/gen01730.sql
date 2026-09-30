WITH RankedAccounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS rn
    FROM accounts
)
SELECT
    id,
    status,
    balance,
    account_type
FROM RankedAccounts
WHERE rn <= 2;
