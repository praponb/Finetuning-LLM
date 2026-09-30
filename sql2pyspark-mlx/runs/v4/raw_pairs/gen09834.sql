select
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    avg(cost) over (order by shipped_date, id rows between 6 preceding and 1 preceding) as cost_avg_6_prior,
    sum(cost) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as running_cost,
    dense_rank() over (partition by status order by cost) as cost_dense_rank
from shipments
