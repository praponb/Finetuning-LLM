with valid_subscriptions as (
    select *
    from subscriptions
    where start_date < '2024-12-01'
),
status_summary as (
    select status, max(start_date) latest_start_date, sum(case when plan = 'premium' then monthly_fee else 0 end) premium_monthly_fee
    from valid_subscriptions
    group by status
)
select *
from status_summary
where premium_monthly_fee > 58
