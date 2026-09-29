select
    id,
    plan,
    monthly_fee,
    status,
    end_date,
    round(monthly_fee / sum(monthly_fee) over (partition by plan), 3) as monthly_fee_share,
    first_value(plan) over (partition by status order by end_date, id) as first_plan
from subscriptions
order by status asc, monthly_fee desc
