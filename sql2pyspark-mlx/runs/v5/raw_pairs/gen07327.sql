with customer_totals as (
    select customer_id, avg(monthly_fee) as avg_monthly_fee, sum(case when plan = 'enterprise' then 1 else 0 end) as enterprise_count
    from subscriptions
    where customer_id is null
    group by customer_id
)
select
    c.id,
    c.signup_date,
    ct.avg_monthly_fee,
    ct.enterprise_count
from customers c
join customer_totals ct on c.id = ct.customer_id
where ct.avg_monthly_fee >= 150
order by avg_monthly_fee desc, c.signup_date;
