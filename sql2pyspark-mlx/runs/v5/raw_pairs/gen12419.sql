SELECT c.segment, AVG(a.balance) AS avg_balance FROM accounts a JOIN customers c ON a.customer_id = c.id GROUP BY c.segment HAVING AVG(a.balance) > 40000;
