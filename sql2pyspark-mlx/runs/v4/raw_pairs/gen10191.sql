select t.is_flagged, t.txn_time, t.txn_type, a.status, a.opened_date, a.account_type from transactions t inner join accounts a on t.account_id = a.id;
