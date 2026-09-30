select
    id,
    customer_id,
    plan,
    monthly_fee,
    avg(monthly_fee) over (partition by customer_id, plan) as customer_id_avg_monthly_fee
from subscriptions
where monthly_fee >= 240
