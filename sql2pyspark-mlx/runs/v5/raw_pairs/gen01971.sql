select quarter(opened_date) as opened_quarter, sum(sqft) as total_sqft, dense_rank() over (order by sum(sqft) desc) as rnk
from stores
group by quarter(opened_date)
order by opened_quarter desc
