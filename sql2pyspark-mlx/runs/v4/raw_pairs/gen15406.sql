select
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    percent_rank() over (partition by customer_id order by monthly_fee desc) as monthly_fee_percent_rank,
    avg(monthly_fee) over (partition by status) as status_avg_monthly_fee,
    round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 4) as monthly_fee_share,
    lead(monthly_fee, 2) over (partition by plan, status order by end_date asc, id asc) as next_monthly_fee,
    last_value(monthly_fee) ignore nulls over (order by end_date asc, id asc rows between unbounded preceding and current row) as last_known_monthly_fee
from subscriptions;
