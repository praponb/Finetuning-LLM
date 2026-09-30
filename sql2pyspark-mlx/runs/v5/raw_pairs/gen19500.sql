SELECT t.id, t.is_flagged, t.merchant, t.txn_type, a.account_type, a.status FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE a.balance BETWEEN 47000 AND 71000
