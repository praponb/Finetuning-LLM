with customer_stats as (
    select customer_id, sum(case when plan = 'standard' then monthly_fee else 0 end) as standard_monthly_fee
    from subscriptions
    where status = 'PAUSED'
    group by customer_id
)
select
    customers.id,
    customers.city,
    customers.segment,
    customer_stats.standard_monthly_fee
from customers
left join customer_stats on customers.id = customer_stats.customer_id
where customer_stats.standard_monthly_fee >= 44
order by customers.city asc;
