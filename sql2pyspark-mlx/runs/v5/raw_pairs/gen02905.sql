SELECT account_type, status FROM accounts WHERE account_type = 'CREDIT' AND customer_id IN (1, 30) ORDER BY account_type, status;
