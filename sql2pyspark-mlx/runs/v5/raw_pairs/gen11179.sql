select p.order_id, o.store_id
from payments p
right join orders o on p.order_id = o.id;
