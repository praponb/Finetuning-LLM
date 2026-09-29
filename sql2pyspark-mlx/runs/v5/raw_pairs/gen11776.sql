select
    id,
    customer_id,
    end_date,
    status,
    avg(monthly_fee) over (partition by plan order by end_date, id rows between 3 preceding and current row) last4_avg_monthly_fee,
    max(monthly_fee) over (partition by plan order by end_date, id rows between 3 preceding and current row) last4_max_monthly_fee,
    min(monthly_fee) over (partition by plan order by end_date, id rows between 3 preceding and current row) last4_min_monthly_fee
from subscriptions
where start_date <= '2023-07-01';
