SELECT c.country, ROUND(STDDEV(a.balance), 2) AS stddev_balance FROM accounts a JOIN customers c ON a.customer_id = c.id GROUP BY c.country ORDER BY stddev_balance DESC;
