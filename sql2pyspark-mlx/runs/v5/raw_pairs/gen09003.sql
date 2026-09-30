select
    id,
    status,
    start_date,
    monthly_fee,
    lag(monthly_fee, 1) over (partition by status order by start_date, id) prev_monthly_fee
from subscriptions
