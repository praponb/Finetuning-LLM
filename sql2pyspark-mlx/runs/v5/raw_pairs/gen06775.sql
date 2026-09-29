with recent_shipments as (
    select *
    from shipments
    where order_id = 2 or cost between 83 and 95
),
agg as (
    select carrier, max(cost) as max_cost
    from recent_shipments
    group by carrier
)
select carrier, max_cost
from agg
where max_cost > 65
