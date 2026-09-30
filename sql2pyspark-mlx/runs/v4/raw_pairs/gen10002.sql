SELECT t.account_id, t.merchant, t.txn_type, a.status, a.customer_id FROM transactions t FULL OUTER JOIN accounts a ON t.account_id = a.id WHERE t.is_flagged
