select
    date_trunc('year', end_date) as year_start,
    customer_id,
    sum(monthly_fee) as total_monthly_fee,
    round(sum(monthly_fee) / nullif(count(case when plan = 'standard' then 1 end), 0), 2) as monthly_fee_per_standard,
    max(monthly_fee) as max_monthly_fee,
    sort_array(collect_set(plan)) as plan_list
from subscriptions
group by 1, 2
order by monthly_fee_per_standard desc;
