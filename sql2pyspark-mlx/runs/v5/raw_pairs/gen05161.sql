with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        datediff(delivered_date, shipped_date) as days_shipped_date_to_delivered_date,
        last_day(shipped_date) as shipped_date_month_end,
        coalesce(carrier, 'N/A') as carrier_filled
    from shipments
    where order_id is not null
)
select carrier, status
from cleaned
except all
select carrier, status
from shipments
where cost >= 39;
