with account_totals as (
    select account_id, count(amount) as amount_count
    from transactions
    where account_id in (10, 24, 27, 30)
    group by account_id
)
select a.balance, at.amount_count
from accounts a
inner join account_totals at on a.id = at.account_id
where at.amount_count >= 8
order by a.balance desc, amount_count desc;
