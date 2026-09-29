SELECT
    id,
    balance,
    opened_date,
    status
FROM accounts
WHERE balance > (SELECT AVG(balance) FROM accounts)
ORDER BY id
