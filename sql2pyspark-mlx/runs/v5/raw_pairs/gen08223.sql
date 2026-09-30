select o.id, o.channel, count(p.id) as payment_count
from orders o
left join payments p on p.order_id = o.id
group by o.id, o.channel
order by channel, payment_count desc, id
limit 10
