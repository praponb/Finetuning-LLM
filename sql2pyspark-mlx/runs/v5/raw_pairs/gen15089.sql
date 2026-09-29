select
    p.channel,
    p.status as order_status,
    a.status as shipment_status,
    count(*) as rows_total,
    max(a.cost) as max_cost
from shipments a
inner join orders p on a.order_id = p.id
group by p.channel, p.status, a.status
order by rows_total desc nulls last;
