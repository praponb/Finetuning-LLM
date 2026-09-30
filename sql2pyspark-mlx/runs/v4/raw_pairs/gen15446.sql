SELECT TO_DATE(txn_time) txn_day, ROUND(AVG(amount), 2) avg_amount, MAX(amount) - MIN(amount) amount_range
FROM transactions
WHERE txn_time < '2023-10-01' OR HOUR(txn_time) BETWEEN 6 AND 10
GROUP BY 1
ORDER BY txn_day ASC;
