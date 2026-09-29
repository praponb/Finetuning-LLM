SELECT customer_id, balance_count FROM (SELECT customer_id, COUNT(balance) AS balance_count FROM accounts GROUP BY customer_id) AS sub WHERE balance_count >= 4
