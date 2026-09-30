select orders.status, round(avg(order_items.unit_price), 2) as avg_unit_price
from order_items
right outer join orders on order_items.order_id = orders.id
left outer join stores on orders.store_id = stores.id
group by orders.status
