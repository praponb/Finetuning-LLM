select
    *,
    row_number() over (partition by order_id order by cost, id) as row_seq,
    ntile(4) over w_part as cost_bucket,
    last_value(carrier, true) over (partition by status, order_id order by cost, id rows between unbounded preceding and current row) as last_known_carrier,
    dense_rank() over (partition by order_id order by cost nulls last) as cost_dense_rank,
    avg(cost) over w_part_2 as cost_avg_3
from shipments
window w_part as (partition by carrier order by cost desc, id), w_part_2 as (order by cost, id rows between 3 preceding and current row);
