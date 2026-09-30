SELECT t.id, t.txn_type, t.amount FROM transactions t WHERE t.amount = (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.txn_type = t.txn_type) ORDER BY t.id DESC;
