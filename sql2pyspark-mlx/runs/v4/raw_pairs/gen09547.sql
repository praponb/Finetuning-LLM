WITH account_stats AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range
    FROM transactions
    WHERE txn_type = 'CREDIT'
    GROUP BY account_id
)
SELECT a.customer_id, a.balance, as.amount_range
FROM accounts a
JOIN account_stats as ON a.id = as.account_id
WHERE as.amount_range > 25;
