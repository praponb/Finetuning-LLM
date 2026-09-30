with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        case when status = 'IN_TRANSIT' then 1 else 0 end as is_in_transit,
        coalesce(carrier, 'N/A') as carrier_clean
    from shipments
    where cost between 26 and 64
)
-- Native SQL PIVOT
select *
from (
    select
        order_id,
        status,
        cost
    from valid
) src
pivot (
    avg(cost)
    for status in ('IN_TRANSIT', 'LOST', 'RETURNED')
);
