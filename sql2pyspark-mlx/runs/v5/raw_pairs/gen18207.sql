select year(launch_date) launch_year, sum(cost) total_cost, sum(sum(cost)) over (order by year(launch_date)) as cumulative_cost
from products
group by year(launch_date)
