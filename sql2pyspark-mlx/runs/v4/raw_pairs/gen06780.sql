select
    *,
    sum(cost) over (partition by order_id, carrier order by delivered_date, id rows between unbounded preceding and current row) as running_cost,
    cume_dist() over (order by cost desc) as cost_cume_dist,
    sum(cost) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as running_cost_2
from shipments;
