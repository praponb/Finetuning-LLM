select product_id, sum(quantity) as total_quantity
from order_items
group by product_id
having sum(quantity) > 29
order by 1;
