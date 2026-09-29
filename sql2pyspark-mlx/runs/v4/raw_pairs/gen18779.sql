with base as (
    select *
    from subscriptions
    where customer_id = 3 and plan in ('standard', 'basic', 'premium') and status = 'EXPIRED'
),
customer_id_summary as (
    select customer_id, max(monthly_fee) max_monthly_fee, sum(monthly_fee) total_monthly_fee
    from base
    group by customer_id
)
select customer_id, total_monthly_fee
from customer_id_summary
where max_monthly_fee < 160
order by customer_id desc, total_monthly_fee desc
