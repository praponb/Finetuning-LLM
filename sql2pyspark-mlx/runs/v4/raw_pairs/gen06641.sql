SELECT id, status, opened_date, account_type, COALESCE(balance, 0) AS balance_filled FROM accounts WHERE account_type NOT IN ('LOAN', 'CREDIT') AND account_type = 'SAVINGS'
