SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount
FROM transactions
GROUP BY account_id
