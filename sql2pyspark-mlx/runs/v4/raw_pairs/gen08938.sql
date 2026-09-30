SELECT a.id, a.status, a.customer_id FROM accounts a WHERE a.status = 'FROZEN' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type IN ('DEBIT', 'REFUND'))
