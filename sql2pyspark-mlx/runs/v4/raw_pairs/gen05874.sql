SELECT c.city, COUNT(a.balance) balance_count FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id GROUP BY c.city ORDER BY balance_count;
