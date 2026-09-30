select avg(units) as avg_units, max(revenue) as max_revenue
from store_sales
where revenue < 3000 or units >= 18
order by avg_units desc
