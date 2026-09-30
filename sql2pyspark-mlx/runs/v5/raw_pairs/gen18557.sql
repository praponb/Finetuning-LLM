SELECT id, customer_id, opened_date, RANK() OVER (PARTITION BY customer_id ORDER BY opened_date) AS rnk FROM accounts WHERE customer_id IN (6, 13, 34, 40) ORDER BY opened_date;
