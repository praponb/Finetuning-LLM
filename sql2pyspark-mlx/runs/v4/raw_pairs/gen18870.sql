SELECT id, status, opened_date, customer_id, COALESCE(balance, 0) AS balance_filled FROM accounts WHERE NOT (status <> 'OPEN' AND opened_date <= '2024-05-01');
