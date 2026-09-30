with valid_accounts as (
    select *
    from accounts
    where status = 'FROZEN' and status = 'FROZEN'
),
agg as (
    select case when balance >= 58000 then 'high' when balance >= 22000 then 'mid' else 'low' end as balance_bucket, count(case when status = 'OPEN' then 1 end) as open_count, min(opened_date) as first_opened_date
    from valid_accounts
    group by case when balance >= 58000 then 'high' when balance >= 22000 then 'mid' else 'low' end
)
select balance_bucket, open_count, first_opened_date
from agg
