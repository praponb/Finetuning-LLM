select
    o.id,
    o.channel,
    count(s.id) as shipment_count,
    round((100 * sum(case when s.carrier = 'FedEx' then 1 else 0 end)) / count(*), 1) as pct_fedex,
    min(s.cost) as min_cost
from orders o
left outer join shipments s on o.id = s.order_id
group by o.id, o.channel
order by shipment_count desc;
