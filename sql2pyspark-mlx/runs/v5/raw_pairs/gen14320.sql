select o.id, o.discount, o.status
from orders o
where o.id in (select order_id from payments)
order by id desc, o.discount asc;
