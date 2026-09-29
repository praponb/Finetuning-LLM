select
    id,
    started_at,
    browser,
    customer_id
from sessions
where customer_id is not null
order by started_at
