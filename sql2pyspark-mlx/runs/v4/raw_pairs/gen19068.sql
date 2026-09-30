SELECT t.txn_type, t.amount, t.is_flagged, a.status FROM transactions t RIGHT JOIN accounts a ON t.account_id = a.id WHERE a.balance BETWEEN 22000 AND 51000 AND a.balance BETWEEN 18000 AND 30000;
