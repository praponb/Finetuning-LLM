select store_id, sum(units) as total_units, rank() over (order by sum(units) desc) as units_rank
from store_sales
where revenue < 2100
group by store_id
