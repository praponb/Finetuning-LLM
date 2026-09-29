select
    id,
    customer_id,
    start_date,
    ntile(2) over (partition by customer_id order by start_date, id) as start_date_ntile
from subscriptions
