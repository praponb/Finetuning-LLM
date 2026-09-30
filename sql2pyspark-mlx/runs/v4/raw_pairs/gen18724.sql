SELECT status, customer_id, CAST(balance AS INT) AS balance_int FROM accounts WHERE account_type = 'CREDIT' AND balance <= 39000 AND MONTH(opened_date) IN (1, 2, 6)
