SELECT t.merchant, t.txn_type, t.amount, a.balance FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE t.is_flagged
