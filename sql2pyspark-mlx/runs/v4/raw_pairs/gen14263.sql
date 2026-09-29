select
    a.id,
    a.balance,
    a.customer_id,
    t.total_amount,
    t.txn_type_list
from accounts a
left join (select account_id, sum(amount) as total_amount, sort_array(collect_set(txn_type)) as txn_type_list
    from transactions
    group by account_id) t on a.id = t.account_id;
