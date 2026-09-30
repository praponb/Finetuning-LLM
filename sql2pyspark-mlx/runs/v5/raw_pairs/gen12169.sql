with ranked as (
    select *
    from device_readings
    qualify row_number() over (partition by device_code order by pressure desc nulls last, id) = 1
)
select
    id,
    humidity,
    pressure,
    status_bits
from ranked
