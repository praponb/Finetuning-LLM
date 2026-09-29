with recent_shipments as (
    select *, date_add(delivered_date, 71) as delivered_date_plus_71d
    from shipments
    where order_id in (5, 7, 20, 23) and carrier = 'Kerry'
),
summary as (
    select order_id, round(stddev(cost), 2) as stddev_cost
    from recent_shipments
    group by order_id
)
select *
from summary
where stddev_cost > 59
