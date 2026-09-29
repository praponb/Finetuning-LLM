SELECT c.id, c.is_active, c.age, t.avg_balance FROM customers c JOIN (SELECT customer_id, ROUND(AVG(balance), 2) AS avg_balance FROM accounts GROUP BY customer_id) t ON c.id = t.customer_id;
