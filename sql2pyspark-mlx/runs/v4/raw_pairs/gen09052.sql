select oi.product_id, o.amount
from order_items oi
join orders o on oi.order_id = o.id
order by oi.product_id;
