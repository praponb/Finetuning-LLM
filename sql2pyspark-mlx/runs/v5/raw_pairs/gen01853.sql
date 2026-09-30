with valid as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        coalesce(humidity, 0) as humidity_filled,
        ceil(temp_c) as temp_c_whole
    from device_readings
    where reading_ts >= date '2025-09-01'
)
select
    id,
    device_code,
    humidity,
    humidity_filled,
    temp_c_whole
from (
    select *, rank() over (partition by device_code order by status_bits) as rn
    from valid
) ranked
where rn = 1;
