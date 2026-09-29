select oi.id, oi.unit_price
from order_items oi
where oi.order_id in (select id from orders where store_id is null)
order by id desc
