select carrier, sum(cost) total_cost, dense_rank() over (order by sum(cost) desc) as cost_rank
from shipments
group by carrier
order by cost_rank desc;
