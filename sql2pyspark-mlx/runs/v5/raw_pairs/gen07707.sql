select month(orders.order_date) order_month, sum(order_items.unit_price) total_unit_price
from order_items
join orders on order_items.order_id = orders.id
group by month(orders.order_date)
having sum(order_items.unit_price) >= 1300;
