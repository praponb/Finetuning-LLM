with filtered_accounts as (
    select *
    from accounts
    where account_type <> 'LOAN' and opened_date < '2025-10-01'
),
agg as (
    select status, count(balance) as balance_count, sum(case when status = 'CLOSED' then 1 else 0 end) as closed_count
    from filtered_accounts
    group by status
)
select status, balance_count, closed_count
from agg
order by closed_count, status
limit 5;
