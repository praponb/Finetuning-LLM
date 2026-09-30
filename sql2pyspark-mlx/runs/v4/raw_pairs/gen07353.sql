select
    s.id,
    s.carrier,
    o.discount,
    o.customer_id
from shipments s
left join orders o on s.order_id = o.id
where o.discount is not null and s.carrier in ('Kerry', 'FedEx', 'DHL')
