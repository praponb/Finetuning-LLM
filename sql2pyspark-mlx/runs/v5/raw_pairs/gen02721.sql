with recent_customers as (
    select *
    from customers
    where segment in ('Consumer', 'Corporate')
),
summary as (
    select date_trunc('MONTH', signup_date) as signup_month, count(credit_limit) as credit_limit_count
    from recent_customers
    group by date_trunc('MONTH', signup_date)
)
select signup_month, credit_limit_count
from summary;
