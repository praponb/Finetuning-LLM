SELECT MONTH(txn_time) AS txn_month, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY MONTH(txn_time)) AS cumulative_amount
FROM transactions
GROUP BY MONTH(txn_time)
ORDER BY total_amount DESC, txn_month
LIMIT 5
