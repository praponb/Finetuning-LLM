select region, sum(sqft) as total_sqft, sum(sum(sqft)) over (order by region) as cumulative_sqft
from stores
where region in ('South', 'Central')
group by region
order by total_sqft, region
limit 10;
