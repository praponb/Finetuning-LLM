select
    o.id,
    o.customer_id,
    o.store_id,
    t.unique_carriers,
    t.avg_cost
from orders o
inner join (select order_id, count(distinct cost) as unique_carriers, avg(cost) as avg_cost
    from shipments
    where order_id in (6, 31, 32, 39)
    group by order_id) t on o.id = t.order_id
order by customer_id desc;
