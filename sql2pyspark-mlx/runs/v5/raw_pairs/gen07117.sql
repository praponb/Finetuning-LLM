SELECT id, customer_id, opened_date, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) AS row_num FROM accounts WHERE account_type IN ('LOAN', 'CREDIT');
