select
    id,
    status,
    start_date,
    monthly_fee,
    lead(monthly_fee, 1) over (partition by status order by start_date, id) next_monthly_fee
from subscriptions
