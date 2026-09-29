with base as (
    select *
    from customers
    where credit_limit >= 15500
),
agg as (
    select quarter(signup_date) as signup_quarter, avg(credit_limit) as avg_credit_limit
    from base
    group by quarter(signup_date)
)
select *
from agg
where avg_credit_limit < 14300
order by avg_credit_limit desc
