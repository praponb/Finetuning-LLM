select product_id, order_id, sum(quantity * unit_price) as revenue
from order_items
group by product_id, order_id
having sum(quantity * unit_price) >= 79
order by product_id, order_id
limit 10;
