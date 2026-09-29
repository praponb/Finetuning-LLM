SELECT
    id,
    status,
    opened_date,
    balance,
    balance - LAG(balance, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS balance_change
FROM accounts
