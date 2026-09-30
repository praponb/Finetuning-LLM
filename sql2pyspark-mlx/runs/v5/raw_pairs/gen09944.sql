with valid_customers as (
    select *
    from customers
    where credit_limit between 5500 and 15500 and (city in ('London', 'Berlin') or segment = 'Corporate')
),
agg as (
    select case when credit_limit >= 17300 then 'high' when credit_limit >= 7400 then 'mid' else 'low' end as credit_limit_bucket, max(signup_date) as latest_signup_date, min(credit_limit) as min_credit_limit
    from valid_customers
    group by case when credit_limit >= 17300 then 'high' when credit_limit >= 7400 then 'mid' else 'low' end
)
select *
from agg
order by latest_signup_date
