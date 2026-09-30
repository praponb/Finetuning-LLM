select
    id,
    end_date,
    monthly_fee,
    round(monthly_fee, 2) as monthly_fee_rounded
from subscriptions
where start_date between '2023-06-01' and '2025-07-01'
order by monthly_fee
