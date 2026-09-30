select p.id, p.paid_at
from payments p
where p.order_id in (select id from orders where shipping_fee >= 37)
order by p.paid_at;
