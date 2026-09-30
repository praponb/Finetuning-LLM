select
    id,
    status,
    monthly_fee,
    customer_id,
    first_value(customer_id) over (partition by status order by monthly_fee, id) first_customer_id
from subscriptions
order by first_customer_id desc, status
