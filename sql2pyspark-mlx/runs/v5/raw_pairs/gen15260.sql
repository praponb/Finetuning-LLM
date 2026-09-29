select s.id, s.shipped_date
from shipments s
where s.order_id in (select id from orders where amount > 1000)
