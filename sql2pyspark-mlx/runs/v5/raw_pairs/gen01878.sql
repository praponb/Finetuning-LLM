with account_summary as (
    select account_id, count(*) cnt, sort_array(collect_set(txn_type)) txn_type_list
    from transactions
    group by account_id
)
select
    a.id,
    a.balance,
    a.opened_date,
    as.cnt,
    as.txn_type_list
from accounts a
left join account_summary as on a.id = as.account_id
where as.cnt > 9;
