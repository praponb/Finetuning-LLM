select
    id,
    customer_id,
    end_date,
    status,
    max(monthly_fee) over w_part as running_max_monthly_fee,
    sum(monthly_fee) over w_part as running_sum_monthly_fee,
    min(monthly_fee) over w_part as running_min_monthly_fee,
    percent_rank() over w_ordered as monthly_fee_percent_rank
from subscriptions
window w_part as (partition by status order by end_date, id rows between unbounded preceding and current row), w_ordered as (order by monthly_fee desc nulls last)
