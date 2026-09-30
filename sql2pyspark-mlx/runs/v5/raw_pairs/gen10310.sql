SELECT account_type, customer_id, COUNT(balance) AS balance_count FROM accounts WHERE account_type IN ('SAVINGS', 'CREDIT', 'LOAN') AND balance <= 66000 GROUP BY 1, 2
