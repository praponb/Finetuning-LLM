with recent_shipments as (
    select *
    from shipments
    where status = 'IN_TRANSIT'
),
summary as (
    select carrier, min(delivered_date) as min_delivered_date
    from recent_shipments
    group by carrier
)
select carrier, min_delivered_date
from summary
order by carrier
limit 10;
