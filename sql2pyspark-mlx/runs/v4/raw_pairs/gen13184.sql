select payments.order_id, orders.status, orders.channel
from payments
inner join orders on orders.id = payments.order_id
where payments.amount > 1400
order by orders.channel;
