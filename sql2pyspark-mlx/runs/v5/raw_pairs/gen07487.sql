select orders.id, orders.channel
from orders
where orders.amount < 810 and orders.customer_id not in (select id from customers where segment in ('Corporate', 'SMB'))
