select
    id,
    start_date,
    end_date,
    first_value(end_date) over (order by start_date, id) as first_end_date
from subscriptions
order by first_end_date
