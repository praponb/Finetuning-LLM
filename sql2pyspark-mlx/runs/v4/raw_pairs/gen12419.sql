SELECT id, customer_id, balance, account_type, LOWER(account_type) AS account_type_lower FROM accounts WHERE NOT ((status = 'FROZEN' AND opened_date <= '2024-07-01') OR customer_id IN (11, 28, 36));
