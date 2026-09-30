SELECT
    id,
    status,
    balance,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) rn
FROM accounts
WHERE account_type NOT IN ('LOAN', 'CREDIT')
