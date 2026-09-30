WITH account_stats AS (
    SELECT account_id, ROUND(AVG(amount), 2) avg_amount, SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) refund_count
    FROM transactions
    WHERE account_id IN (18, 29, 33)
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.customer_id,
    as.avg_amount,
    as.refund_count
FROM accounts a
LEFT OUTER JOIN account_stats as ON a.id = as.account_id
WHERE as.refund_count > 41;
