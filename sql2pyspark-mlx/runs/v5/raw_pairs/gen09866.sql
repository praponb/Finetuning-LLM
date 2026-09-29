with account_totals as (
    select account_id, count(amount) as amount_count
    from transactions
    group by account_id
)
select a.id, a.account_type, at.amount_count
from accounts a
left join account_totals at on a.id = at.account_id
