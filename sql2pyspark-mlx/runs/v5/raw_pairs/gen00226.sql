SELECT
    id,
    account_type,
    opened_date,
    balance
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN', 'CREDIT')
