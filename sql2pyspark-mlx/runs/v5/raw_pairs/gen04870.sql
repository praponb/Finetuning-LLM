select
    e.*,
    sub.id as subscription_id,
    sub.plan as subscription_plan,
    sub.monthly_fee as subscription_monthly_fee
from subscriptions e
left join subscriptions sub
    on e.customer_id = sub.customer_id
   and e.start_date >= sub.start_date
   and e.start_date < sub.end_date + interval 14 days
