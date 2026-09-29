with recent_shipments as (
    select *, trim(carrier) as clean_carrier
    from shipments
    where carrier = 'Kerry'
),
agg as (
    select status, sum(cost) as total_cost, round((100 * sum(case when status = 'DELIVERED' then 1 else 0 end)) / count(*), 1) as pct_delivered
    from recent_shipments
    group by status
)
select *
from agg
order by pct_delivered desc
