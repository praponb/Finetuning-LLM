SELECT status, ROUND(AVG(balance), 2) AS avg_balance, SUM(balance) AS total_balance
FROM accounts
WHERE customer_id IS NOT NULL AND opened_date >= '2024-01-20'
GROUP BY 1
HAVING SUM(balance) >= 358000
ORDER BY avg_balance DESC, total_balance DESC;
