with recent_subscriptions as (
    select *
    from subscriptions
    where customer_id in (2, 27, 32, 37) or monthly_fee > 220
),
subscriptions_stats as (
    select plan, status, sum(case when plan = 'premium' then monthly_fee else 0 end) as premium_monthly_fee
    from recent_subscriptions
    group by plan, status
)
select *
from subscriptions_stats
where premium_monthly_fee >= 68
order by 1
