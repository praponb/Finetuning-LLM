select s.id, s.delivered_date
from shipments s
where s.carrier in ('Kerry', 'DHL') and s.order_id in (select id from orders where channel in ('web', 'partner'));
