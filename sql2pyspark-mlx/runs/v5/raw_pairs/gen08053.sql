with recent_subscriptions as (
    select *, date_sub(start_date, 60) as start_date_minus_60d
    from subscriptions
    where month(start_date) in (9, 11, 12)
),
subscriptions_stats as (
    select
        year(start_date) as start_year,
        case when status in ('CANCELLED', 'PAUSED') then 'core' else 'other' end as status_group,
        avg(monthly_fee) as avg_monthly_fee,
        count(distinct monthly_fee) as unique_plans
    from recent_subscriptions
    group by year(start_date), case when status in ('CANCELLED', 'PAUSED') then 'core' else 'other' end
)
select
    start_year,
    status_group,
    avg_monthly_fee,
    unique_plans
from subscriptions_stats
where avg_monthly_fee < 80
order by 3 desc, unique_plans desc, start_year, status_group
limit 50;
