select e.id, e.account_id, e.txn_type, e.is_flagged, a.status, a.balance, a.account_type from transactions e left join accounts a on e.account_id = a.id
