SELECT
    id,
    status,
    opened_date,
    balance,
    LAG(balance, 1, 0) OVER (PARTITION BY status ORDER BY opened_date, id) prev_balance
FROM accounts
WHERE balance >= 64000;
