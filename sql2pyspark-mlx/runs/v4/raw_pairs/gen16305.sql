select channel, max(order_date) latest_order_date
from orders
where status = 'CANCELLED'
group by 1
