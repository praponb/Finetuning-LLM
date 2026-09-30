select
    id,
    customer_id,
    end_date,
    rank() over (partition by customer_id order by end_date) end_date_rank
from subscriptions
