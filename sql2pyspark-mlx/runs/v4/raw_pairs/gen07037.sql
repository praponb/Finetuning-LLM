select
    o.id,
    o.customer_id,
    t.latest_delivered_date,
    t.unique_carriers
from orders o
left join (select order_id, max(delivered_date) as latest_delivered_date, count(distinct cost) as unique_carriers
    from shipments
    group by order_id) t on o.id = t.order_id
