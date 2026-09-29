with filtered_subscriptions as (
    select *
    from subscriptions
    where customer_id = 22 and (status in ('EXPIRED', 'CANCELLED', 'PAUSED') or monthly_fee >= 140)
),
agg as (
    select customer_id, sum(monthly_fee) total_monthly_fee, max(start_date) last_start_date
    from filtered_subscriptions
    group by customer_id
)
select *
from agg
order by last_start_date;
