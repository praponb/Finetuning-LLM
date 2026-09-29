SELECT t.id, t.txn_type, t.account_id FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE opened_date BETWEEN '2023-04-01' AND '2024-08-01') ORDER BY t.txn_type, id LIMIT 5;
