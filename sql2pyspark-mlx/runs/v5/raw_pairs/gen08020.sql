with AccountSummary as (
    select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values, sum(amount) as total_amount
    from transactions
    group by account_id
)
select
    a.id,
    a.customer_id,
    A.txn_type_values,
    A.total_amount
from accounts a
join AccountSummary A on a.id = A.account_id;
