select
    id,
    customer_id,
    start_date,
    monthly_fee,
    status,
    avg(monthly_fee) over w_recent as running_avg_monthly_fee,
    max(monthly_fee) over w_recent as running_max_monthly_fee,
    sum(monthly_fee) over w_recent as running_sum_monthly_fee,
    ntile(10) over (partition by plan order by monthly_fee desc, id) as monthly_fee_bucket
from subscriptions
window w_recent as (partition by customer_id, plan order by monthly_fee, id rows between unbounded preceding and current row);
