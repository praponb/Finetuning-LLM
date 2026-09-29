SELECT
    id,
    status,
    account_type,
    balance,
    ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance, id) AS rn
FROM accounts
ORDER BY status, account_type;
