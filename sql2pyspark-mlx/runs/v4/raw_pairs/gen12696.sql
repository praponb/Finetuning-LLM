SELECT ROUND(AVG(balance), 2) AS avg_balance FROM accounts WHERE customer_id IS NOT NULL ORDER BY avg_balance DESC;
