with order_stats as (
    select order_id, count(case when status = 'IN_TRANSIT' then 1 end) as in_transit_count
    from shipments
    group by order_id
)
select o.id, o.order_date, os.in_transit_count
from orders o
left join order_stats os on o.id = os.order_id
where os.in_transit_count >= 5
