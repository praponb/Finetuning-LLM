select category, max(launch_date) as last_launch_date, count(*) as product_count
from products
where year(launch_date) = 2023 and stock_qty <= 274 and size(tags) > 1
group by category
order by category desc
limit 5
