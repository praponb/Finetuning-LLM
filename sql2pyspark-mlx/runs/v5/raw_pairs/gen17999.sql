select t.id, t.txn_time, t.txn_type, t.account_id, a.status, a.opened_date from transactions t inner join accounts a on t.account_id = a.id where t.merchant = 'Grab'
