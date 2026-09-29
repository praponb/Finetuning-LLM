select o.status, count(distinct p.amount) as unique_methods
from payments p
join orders o on p.order_id = o.id
group by o.status
having count(distinct p.amount) >= 1;
