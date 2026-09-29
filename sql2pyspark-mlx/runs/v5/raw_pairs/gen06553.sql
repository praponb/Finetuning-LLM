select a.customer_id, a.account_type, t.txn_type, t.txn_time, t.account_id from accounts a inner join transactions t on a.id = t.account_id
