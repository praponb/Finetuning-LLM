SELECT
    id,
    account_type,
    status,
    balance,
    ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY balance, id) AS rn
FROM accounts
WHERE balance <= 57000
