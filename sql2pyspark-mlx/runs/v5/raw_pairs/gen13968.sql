with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        lower(firmware) firmware_lower
    from device_readings
    where status_bits < 7
)
select id
from cleaned
except all
select id
from device_readings
where status_bits >= 5
