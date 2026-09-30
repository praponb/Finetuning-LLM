select
    o.id,
    o.status,
    count(s.id) as shipment_count,
    count(case when s.carrier = 'Flash' then 1 end) as flash_count
from orders o
left outer join shipments s on s.order_id = o.id
group by o.id, o.status
order by flash_count desc;
