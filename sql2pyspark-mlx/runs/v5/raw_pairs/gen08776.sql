WITH account_summary AS (
    SELECT account_id, MAX(amount) AS max_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.balance, as.max_amount
FROM accounts a
JOIN account_summary as ON a.id = as.account_id
WHERE as.max_amount > 3300
ORDER BY balance
