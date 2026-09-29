SELECT merchant, MIN(amount) min_amount
FROM transactions
WHERE txn_time >= '2023-06-01' AND is_flagged AND txn_type IN ('FEE', 'DEBIT')
GROUP BY merchant
ORDER BY min_amount DESC
