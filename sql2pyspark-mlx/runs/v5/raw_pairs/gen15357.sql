with ranked as (
    select *, row_number() over (partition by status, plan order by start_date nulls last, id) as rn
    from subscriptions
    qualify rn = 1
)
select
    case when monthly_fee >= 231 then 'LARGE' else 'SMALL' end as monthly_fee_tier,
    date_trunc('week', start_date) as week_start,
    round(avg(monthly_fee), 2) as avg_monthly_fee,
    avg(monthly_fee) filter (where status = 'CANCELLED') as cancelled_avg_monthly_fee,
    round(avg(monthly_fee), 2) as avg_monthly_fee_2
from ranked
group by 1, 2
order by monthly_fee_tier desc;
