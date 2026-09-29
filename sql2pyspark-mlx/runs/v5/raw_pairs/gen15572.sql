with base as (
    select *
    from shipments
    where carrier = 'DHL' or carrier <> 'UPS'
),
order_id_summary as (
    select order_id, sum(case when status = 'IN_TRANSIT' then cost else 0 end) as in_transit_cost
    from base
    group by order_id
)
select order_id, in_transit_cost
from order_id_summary
order by in_transit_cost desc
