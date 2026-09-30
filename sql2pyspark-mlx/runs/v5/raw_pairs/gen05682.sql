select
    s.delivered_date,
    s.status,
    s.order_id,
    o.shipping_fee
from shipments s
left outer join orders o on s.order_id = o.id
order by shipping_fee;
