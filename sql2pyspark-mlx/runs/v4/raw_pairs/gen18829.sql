with account_totals as (
    select account_id, sum(case when merchant = 'Amazon' then amount else 0 end) as amazon_amount, sum(amount) as total_amount
    from transactions
    group by account_id
)
select a.balance, at.amazon_amount, at.total_amount
from accounts a
left join account_totals at on a.id = at.account_id
where at.total_amount >= 14000
order by total_amount;
