with filtered as (
    select *, datediff(current_date(), start_date) as days_since_start_date
    from subscriptions
    where end_date >= to_date('2023-06-22')
)
select
    *,
    min(monthly_fee) over w_part as running_min_monthly_fee,
    sum(monthly_fee) over w_part as running_sum_monthly_fee,
    count(monthly_fee) over w_part as running_count_monthly_fee,
    max(monthly_fee) over w_part as running_max_monthly_fee,
    sum(days_since_start_date) over (order by days_since_start_date, id rows between unbounded preceding and current row) as running_days_since_start_date
from filtered
window w_part as (partition by status order by days_since_start_date, id rows between unbounded preceding and current row);
