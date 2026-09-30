SELECT id, account_type, balance, opened_date FROM accounts QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) = 1
