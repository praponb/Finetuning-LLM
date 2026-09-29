with with_dims as (
    -- Attach orders attributes
    select s.*, p.amount
    from shipments s
    left join orders p
        on s.order_id = p.id
)
-- Multi-level aggregation
select
    date_trunc('week', shipped_date) as week_start,
    status,
    case when cost >= 87 then 'HIGH' else 'LOW' end as cost_tier,
    count_if(carrier = 'Flash') as flash_events,
    count(*) as num_rows,
    count(*) filter (where status = 'IN_TRANSIT') as in_transit_rows,
    avg(amount) filter (where status = 'DELIVERED') as delivered_avg_amount
from with_dims
group by week_start, status, cost_tier
order by in_transit_rows desc nulls last;
