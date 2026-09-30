with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        trim(firmware) as clean_firmware
    from device_readings
    where reading_ts > date '2025-09-01'
)
select firmware, id
from filtered
except
select firmware, id
from device_readings
where firmware <> '1.0.3';
