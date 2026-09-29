SELECT id, status, customer_id, account_type FROM accounts WHERE customer_id IN (5, 29, 40) AND opened_date <= '2024-04-01' ORDER BY id;
