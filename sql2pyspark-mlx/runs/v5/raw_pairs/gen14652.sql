select s.id, s.delivered_date, o.channel
from shipments s
left join orders o on s.order_id = o.id;
