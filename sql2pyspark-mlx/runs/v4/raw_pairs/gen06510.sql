select o.id, o.channel
from orders o
where o.customer_id in (select id from customers where country = 'DE')
order by 1 asc
