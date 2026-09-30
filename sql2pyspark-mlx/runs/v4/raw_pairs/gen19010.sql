with filtered_customers as (
    select *
    from customers
    where email like '%@company.co'
),
agg as (
    select month(signup_date) as signup_month, max(signup_date) as latest_signup_date
    from filtered_customers
    group by month(signup_date)
)
select signup_month, latest_signup_date
from agg;
