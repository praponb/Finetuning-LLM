SELECT account_id, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM transactions
GROUP BY account_id
ORDER BY pct_credit
