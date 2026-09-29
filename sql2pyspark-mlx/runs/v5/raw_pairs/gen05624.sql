select
    id,
    order_id,
    cost,
    row_number() over (partition by status, carrier order by cost desc, id) as row_seq,
    rank() over (order by cost desc nulls last) as cost_rank,
    lead(cost, 1) over (partition by status, order_id order by cost, id) as next_cost
from shipments
