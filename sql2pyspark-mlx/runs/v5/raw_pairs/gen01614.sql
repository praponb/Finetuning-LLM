select o.id, o.status, count(oi.id) as order_item_count
from orders o
left outer join order_items oi on oi.order_id = o.id
group by o.id, o.status
having order_item_count >= 1
order by o.id desc
