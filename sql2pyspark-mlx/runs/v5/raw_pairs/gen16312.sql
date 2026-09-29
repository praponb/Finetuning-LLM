SELECT a.id, a.txn_type, a.amount, a.account_id FROM transactions a WHERE a.amount = (SELECT MAX(b.amount) FROM transactions b WHERE b.txn_type = a.txn_type)
