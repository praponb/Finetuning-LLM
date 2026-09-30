SELECT id, account_type, customer_id, opened_date, balance, FIRST_VALUE(balance) OVER (PARTITION BY account_type, customer_id ORDER BY opened_date DESC, id) AS first_balance FROM accounts
