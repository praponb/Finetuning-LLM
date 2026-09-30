with valid as (
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        round(cost * 100, 2) as cost_scaled
    from shipments
    where carrier is not null
      and delivered_date > to_date('2023-05-20')
),
valid_shipments as (
    select
        id,
        carrier,
        delivered_date,
        cost,
        status,
        cost_scaled,
        initcap(trim(status)) as status_title,
        coalesce(carrier, 'UNKNOWN') as resolved_carrier
    from valid
    where delivered_date > to_date('2023-09-25')
      and carrier is distinct from 'Flash'
),
base as (
    select
        *,
        cost_scaled * cost as cost_scaled_times_cost,
        coalesce(carrier, 'NONE') as carrier_filled
    from valid_shipments
    where carrier is not null
      and carrier in ('Flash', 'UPS')
),
latest as (
    select *, rank() over (partition by status, carrier order by delivered_date desc nulls first) as rn
    from base
    qualify rn = 1
)
select *
from (
    select
        carrier,
        status,
        cost
    from latest
) src
pivot (
    avg(cost) as total,
    count(1) as item_count
    for status in ('LOST', 'IN_TRANSIT')
)
