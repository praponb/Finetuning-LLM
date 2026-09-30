with RecentShipments as (
    select *
    from shipments
    where order_id is not null
),
carrier_summary as (
    select
        carrier,
        status,
        sum(case when status = 'LOST' then cost else 0 end) as lost_cost,
        count(case when status = 'IN_TRANSIT' then 1 end) as in_transit_count
    from RecentShipments
    group by carrier, status
)
select *
from carrier_summary
order by lost_cost, in_transit_count desc, carrier, status
limit 1;
