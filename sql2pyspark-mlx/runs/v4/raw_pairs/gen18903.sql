with transactions_by_account as (
    select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) txn_type_values
    from transactions
    where is_flagged = false
    group by account_id
)
select a.id, a.customer_id, tba.txn_type_values
from accounts a
join transactions_by_account tba on a.id = tba.account_id
order by tba.txn_type_values, id
limit 3
