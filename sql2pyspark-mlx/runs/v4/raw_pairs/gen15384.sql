SELECT TO_DATE(txn_time) AS txn_day, COUNT(amount) AS amount_count
FROM transactions
WHERE txn_time < '2024-02-25'
GROUP BY txn_day
ORDER BY txn_day
LIMIT 5;
