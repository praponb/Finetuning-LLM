select p.amount, o.discount, o.order_date
from payments p
full outer join orders o on p.order_id = o.id
order by o.order_date
