with AccountStats as (
    select account_id, sort_array(collect_set(merchant)) as merchant_list, max(amount) - min(amount) as amount_range
    from transactions
    group by account_id
)
select
    a.balance,
    a.account_type,
    A.merchant_list,
    A.amount_range
from accounts a
left join AccountStats A on a.id = A.account_id
order by account_type asc
