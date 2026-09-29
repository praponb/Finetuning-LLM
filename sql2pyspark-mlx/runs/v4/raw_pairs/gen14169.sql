SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, AVG(amount) AS avg_amount
FROM transactions
GROUP BY txn_month;
