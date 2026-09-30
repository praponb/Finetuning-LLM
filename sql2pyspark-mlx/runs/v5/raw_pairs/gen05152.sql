select
    status,
    sum(cost) as total_cost,
    count(*) as cnt,
    sum(sum(cost)) over (order by status) as cumulative_cost
from shipments
group by status
order by cumulative_cost desc, status
limit 3;
