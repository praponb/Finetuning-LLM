SELECT TO_DATE(txn_time) AS txn_day, MIN(amount) AS min_amount
FROM transactions
WHERE (amount <= 3100 AND txn_type IN ('FEE', 'CREDIT', 'REFUND')) OR amount < 2500
GROUP BY TO_DATE(txn_time)
HAVING MIN(amount) < 1800
ORDER BY 1 ASC
