SELECT a.id, a.balance, a.customer_id FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'DEBIT') ORDER BY id DESC, customer_id DESC
