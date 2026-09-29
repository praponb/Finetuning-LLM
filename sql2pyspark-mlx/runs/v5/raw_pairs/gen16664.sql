with account_totals as (
    select account_id, sum(amount) total_amount
    from transactions
    where amount between 1300 and 2500
    group by account_id
)
select a.opened_date, a.status, at.total_amount
from accounts a
inner join account_totals at on a.id = at.account_id;
