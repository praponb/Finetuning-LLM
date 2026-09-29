select oi.quantity, o.amount, o.customer_id
from order_items oi
left join orders o on oi.order_id = o.id;
