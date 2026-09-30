select
    id,
    order_id,
    paid_at,
    percent_rank() over (partition by order_id order by paid_at) as paid_at_pct_rank
from payments;
