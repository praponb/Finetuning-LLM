select o.id, count(p.id) as payment_count, max(p.amount) - min(p.amount) as amount_range
from orders o
left outer join payments p on p.order_id = o.id
group by o.id
order by payment_count desc, id
limit 10
