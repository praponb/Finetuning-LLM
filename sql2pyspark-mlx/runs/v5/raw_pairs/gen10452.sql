select a.customer_id, a.balance, t.min_txn_time
from accounts a
inner join (select account_id, min(txn_time) as min_txn_time
    from transactions
    group by account_id) t on a.id = t.account_id
