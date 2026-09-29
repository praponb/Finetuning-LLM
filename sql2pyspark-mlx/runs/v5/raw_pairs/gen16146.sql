with recent_accounts as (
    select *, lower(status) as status_lower
    from accounts
    where balance > 28000 and (opened_date < '2023-06-01' or account_type = 'CREDIT')
),
Summary as (
    select case when balance >= 86000 then 'high' when balance >= 23000 then 'mid' else 'low' end as balance_bucket, round((100 * sum(case when status = 'OPEN' then 1 else 0 end)) / count(*), 1) as pct_open
    from recent_accounts
    group by case when balance >= 86000 then 'high' when balance >= 23000 then 'mid' else 'low' end
)
select *
from Summary
where pct_open >= 55
order by pct_open
