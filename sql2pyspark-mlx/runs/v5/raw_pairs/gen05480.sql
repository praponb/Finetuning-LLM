with dim_totals as (
    select
        o.channel,
        e.status as shipment_status,
        count(*) as num_rows,
        min(e.cost) as min_cost,
        grouping_id() as level_id
    from shipments e
    join orders o on e.order_id = o.id
    group by rollup (o.channel, e.status)
)
select
    channel,
    shipment_status,
    num_rows,
    min_cost,
    level_id,
    case when shipment_status rlike '^(LOST|RETURNED)$' then 'OTHER_ZONE' when shipment_status rlike '^(IN_TRANSIT|DELIVERED)$' then 'GROUP_B' else 'EXTENDED' end as shipment_status_zone
from dim_totals
order by shipment_status desc nulls last;
