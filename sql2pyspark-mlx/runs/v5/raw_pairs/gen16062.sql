SELECT is_flagged, AVG(amount) AS avg_amount
FROM transactions
WHERE txn_type IN ('CREDIT', 'DEBIT') OR amount <= 2200
GROUP BY is_flagged
