SELECT merchant, SUM(amount) total_amount, MAX(txn_time) latest_txn_time
FROM transactions
WHERE merchant IN ('7-Eleven', 'Amazon', 'Shell')
GROUP BY merchant
HAVING COUNT(DISTINCT amount) > 2
ORDER BY 3 DESC, total_amount
