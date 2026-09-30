WITH account_summary AS (
    SELECT account_id, COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count, COUNT(amount) AS amount_count
    FROM transactions
    WHERE merchant = '7-Eleven'
    GROUP BY account_id
)
SELECT a.customer_id, as.credit_count, as.amount_count
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
WHERE as.amount_count >= 4;
