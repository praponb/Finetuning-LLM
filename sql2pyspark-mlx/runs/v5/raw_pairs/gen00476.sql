select
    id,
    customer_id,
    started_at,
    rank() over (partition by customer_id order by started_at desc) as rnk
from sessions
where to_date(started_at) < '2023-08-01'
order by customer_id, started_at
