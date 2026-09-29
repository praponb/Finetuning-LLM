select
    id,
    plan,
    customer_id,
    start_date,
    date_add(end_date, 75) as end_date_plus_75d,
    case when monthly_fee >= 247 then 'High' when monthly_fee >= 95 then 'Medium' else 'Low' end as monthly_fee_band
from subscriptions
