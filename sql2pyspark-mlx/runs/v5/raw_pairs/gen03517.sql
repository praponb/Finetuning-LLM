SELECT MONTH(opened_date) opened_month, SUM(balance) total_balance
FROM accounts
WHERE balance >= 53000
GROUP BY MONTH(opened_date)
HAVING total_balance < 449000
ORDER BY total_balance, opened_month
LIMIT 5;
