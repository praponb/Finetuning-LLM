with deduped as (
    select *, row_number() over (partition by carrier order by cost desc, id) as rank_in_group
    from shipments
    qualify rank_in_group = 1
)
select
    status,
    case when cost >= 88 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    order_id,
    count(*) as num_rows,
    sum(case when carrier = 'DHL' then cost else 0 end) as dhl_cost,
    size(collect_set(status)) as num_status,
    sum(case when status = 'IN_TRANSIT' then cost else 0 end) as in_transit_cost,
    percent_rank() over (partition by status order by count(*) desc) as num_rows_percent_rank
from deduped
group by status, case when cost >= 88 then 'PREMIUM' else 'STANDARD' end, order_id
having dhl_cost > 0
