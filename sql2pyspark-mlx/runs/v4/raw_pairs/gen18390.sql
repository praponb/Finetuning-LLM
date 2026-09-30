SELECT a.status, t.is_flagged, t.txn_time FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE a.customer_id IS NULL AND a.customer_id = 37 ORDER BY a.status, t.txn_time;
