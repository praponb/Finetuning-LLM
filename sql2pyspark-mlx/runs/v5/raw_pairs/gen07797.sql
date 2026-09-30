select
    id,
    order_id,
    cost,
    round(cost / sum(cost) over (partition by order_id), 3) as cost_share
from shipments
where status in ('LOST', 'DELIVERED', 'IN_TRANSIT');
