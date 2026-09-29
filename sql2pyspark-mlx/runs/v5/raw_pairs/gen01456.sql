SELECT t.id, t.account_id, t.txn_type, a.opened_date FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id
