with filtered as (
    select
        id,
        order_id,
        carrier,
        cost,
        status,
        concat(carrier, ', ', status) as carrier_status,
        date_trunc('MONTH', shipped_date) as shipped_date_period
    from shipments
    where order_id is not null
      and carrier is distinct from 'DHL'
)
select
    id,
    cost,
    status
from filtered
qualify row_number() over (partition by status order by shipped_date_period asc, id asc) = 1;
