select
    carrier,
    sum(cost) as total_cost,
    round(avg(cost), 2) as avg_cost,
    concat_ws(', ', sort_array(collect_set(status))) as status_values
from shipments
group by carrier
