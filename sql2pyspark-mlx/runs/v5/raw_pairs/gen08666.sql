select
    o.id,
    o.customer_id,
    o.channel,
    o.shipping_fee
from orders o
where o.discount <= 0.18 and not exists (select 1 from payments p where p.order_id = o.id)
