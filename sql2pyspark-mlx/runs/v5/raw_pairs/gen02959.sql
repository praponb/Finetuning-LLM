SELECT MIN(amount) AS min_amount, ROUND((100 * SUM(CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_debit
FROM transactions
ORDER BY pct_debit DESC
