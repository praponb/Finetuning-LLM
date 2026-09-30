SELECT id, customer_id, status, opened_date, LEAD(opened_date, 2) OVER (PARTITION BY customer_id, status ORDER BY opened_date, id) AS next_opened_date FROM accounts WHERE customer_id IN (2, 5)
