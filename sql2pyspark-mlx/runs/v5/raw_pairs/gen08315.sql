select
    order_id,
    size(collect_set(carrier)) as num_carriers,
    avg(cost) filter (where carrier = 'Flash') as flash_avg_cost,
    approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles
from shipments
group by order_id
order by flash_avg_cost desc;
