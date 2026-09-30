select product_id, count(*) as total_count, max(quantity) as max_quantity
from inventory
group by product_id
order by total_count desc
