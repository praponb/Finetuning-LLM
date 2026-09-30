SELECT id, status, customer_id, opened_date, LEAD(opened_date, 1) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id) AS next_opened_date FROM accounts
