select t.id, t.is_flagged, t.txn_time, t.amount, a.balance, a.status, a.account_type from transactions t left outer join accounts a on t.account_id = a.id;
