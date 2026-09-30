select
    id,
    plan,
    start_date,
    monthly_fee,
    status,
    first_value(monthly_fee) over (partition by plan order by start_date, id) as first_monthly_fee,
    round(monthly_fee / sum(monthly_fee) over (partition by status), 3) as monthly_fee_share
from subscriptions;
