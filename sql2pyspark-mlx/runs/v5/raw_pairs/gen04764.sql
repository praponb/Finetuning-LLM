SELECT
    id,
    account_type,
    opened_date,
    balance,
    LAG(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance
FROM accounts
WHERE account_type = 'CREDIT'
