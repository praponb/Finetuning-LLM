with account_totals as (
    select account_id, concat_ws(', ', sort_array(collect_set(merchant))) merchant_values
    from transactions
    where amount > 1400
    group by account_id
)
select
    a.id,
    a.customer_id,
    a.account_type,
    at.merchant_values
from accounts a
inner join account_totals at on a.id = at.account_id;
