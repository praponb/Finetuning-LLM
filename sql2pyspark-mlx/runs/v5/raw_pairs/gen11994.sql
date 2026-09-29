select t.txn_type, t.amount, t.is_flagged, a.customer_id, a.opened_date, a.balance from transactions t left outer join accounts a on t.account_id = a.id order by t.txn_type;
