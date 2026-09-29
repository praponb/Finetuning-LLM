WITH account_stats AS (
    SELECT account_id, SUM(amount) AS total_amount, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.status, as.total_amount, as.debit_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.debit_amount >= 51
ORDER BY as.debit_amount DESC, total_amount DESC;
