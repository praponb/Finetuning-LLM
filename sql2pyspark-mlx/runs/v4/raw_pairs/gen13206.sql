select oi.id, oi.product_id, oi.unit_price
from order_items oi
where oi.order_id in (select id from orders where customer_id = 18)
