select category, count(*) as product_count, min(launch_date) as first_launch_date
from products
group by category
having count(*) >= 2
order by product_count desc, first_launch_date;
