SELECT t.id, t.account_id FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE customer_id IN (6, 17, 22))
