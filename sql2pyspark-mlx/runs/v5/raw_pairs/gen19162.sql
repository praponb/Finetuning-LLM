SELECT t.id, t.is_flagged, a.balance, a.status, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE HOUR(t.txn_time) BETWEEN 6 AND 11 ORDER BY balance DESC, status;
