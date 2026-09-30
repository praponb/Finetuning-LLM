select o.id, o.shipping_fee, o.customer_id
from orders o
where not exists (select 1 from shipments s where s.order_id = o.id and s.carrier = 'FedEx')
order by shipping_fee desc, id desc
