select orders.id, orders.channel
from orders
where exists (select 1 from payments where payments.order_id = orders.id);
