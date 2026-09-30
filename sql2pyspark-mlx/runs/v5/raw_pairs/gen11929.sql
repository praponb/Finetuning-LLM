SELECT id, balance, status, opened_date FROM accounts WHERE customer_id IS NOT NULL AND status != 'CLOSED' AND balance < 55000;
