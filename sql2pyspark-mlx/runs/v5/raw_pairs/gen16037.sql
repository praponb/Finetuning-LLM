SELECT id, balance, UPPER(account_type) AS account_type_upper FROM accounts WHERE status = 'CLOSED' AND (customer_id = 30 OR status IS NULL);
