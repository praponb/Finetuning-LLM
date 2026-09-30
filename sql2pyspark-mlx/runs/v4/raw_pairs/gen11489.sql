select product_id, count(*) total_count
from inventory
group by product_id
having count(*) >= 1
