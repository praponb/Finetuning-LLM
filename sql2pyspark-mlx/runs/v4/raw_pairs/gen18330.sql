SELECT customer_id, COUNT(DISTINCT balance) AS unique_customers, AVG(balance) AS avg_balance FROM accounts GROUP BY customer_id ORDER BY avg_balance DESC;
