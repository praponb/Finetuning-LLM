select order_id, avg(quantity) as avg_quantity
from order_items
group by order_id
having avg(quantity) < 4.1
order by order_id desc
limit 5;
