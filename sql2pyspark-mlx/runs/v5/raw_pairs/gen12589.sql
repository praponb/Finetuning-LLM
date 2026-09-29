with deduped as (
    select *, rank() over (partition by plan order by start_date) as row_num
    from subscriptions
    qualify row_num = 1
)
select
    date_trunc('week', start_date) as week_start,
    sum(monthly_fee) as total_monthly_fee,
    size(collect_set(plan)) as num_plans,
    count(case when status = 'CANCELLED' then 1 end) as cancelled_count
from deduped
group by week_start
having cancelled_count < 1000
order by week_start desc
