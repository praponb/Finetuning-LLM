with src as (
    select *
    from shipments
    where order_id is not null or status = 'LOST'
),
status_summary as (
    select
        status,
        carrier,
        max(cost) as max_cost,
        max(cost) - min(cost) as cost_range
    from src
    group by status, carrier
)
select *
from status_summary
where cost_range < 68
order by cost_range, max_cost desc;
