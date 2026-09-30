SELECT t.id, t.account_id, t.merchant FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE account_type = 'CREDIT') ORDER BY t.account_id ASC, id ASC LIMIT 20;
