SELECT a.id, a.txn_type, a.amount, a.merchant FROM transactions a WHERE a.amount = (SELECT MAX(b.amount) FROM transactions b WHERE b.txn_type = a.txn_type);
