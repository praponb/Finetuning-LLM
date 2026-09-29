with valid as (
    -- Basic cleansing and defaults
    select
        *,
        lower(status) as status_lower,
        last_day(shipped_date) as shipped_date_month_end,
        coalesce(carrier, 'UNKNOWN') as carrier_clean
    from shipments
    where carrier is not null
)
select carrier
from valid
except all
select carrier
from shipments
where order_id in (8, 18, 25);
