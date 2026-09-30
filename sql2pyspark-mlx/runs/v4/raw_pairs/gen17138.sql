select order_id, sum(cost) as total_cost, round(sum(cost) * 100.0 / sum(sum(cost)) over (), 2) as pct_of_total
from shipments
group by order_id
order by total_cost desc;
