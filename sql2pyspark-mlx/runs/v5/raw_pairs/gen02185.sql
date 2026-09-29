with deduped as (
    select
        id,
        device_code,
        humidity,
        status_bits,
        firmware
    from (
        select *, row_number() over (partition by device_code order by pressure desc nulls last, id) as latest_rank
        from device_readings
    ) x
    where latest_rank = 1
)
select
    id,
    humidity,
    status_bits,
    firmware
from deduped;
