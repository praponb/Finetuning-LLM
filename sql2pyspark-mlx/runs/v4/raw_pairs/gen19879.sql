SELECT t.id, t.txn_type FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE YEAR(opened_date) >= 2024);
