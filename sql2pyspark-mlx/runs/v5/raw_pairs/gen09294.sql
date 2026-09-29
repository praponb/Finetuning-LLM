select id, start_date
from subscriptions
qualify row_number() over (partition by customer_id order by start_date desc nulls last, id) = 1;
