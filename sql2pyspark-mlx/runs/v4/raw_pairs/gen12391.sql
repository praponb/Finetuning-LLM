select
    id,
    customer_id,
    started_at,
    dense_rank() over (partition by customer_id order by started_at) as dense_rnk
from sessions;
