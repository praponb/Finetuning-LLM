with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        lower(device_code) as device_code_lower,
        trim(firmware) as clean_firmware
    from device_readings
    where reading_ts >= timestamp '2025-09-01 16:00:00'
      and to_date(reading_ts) >= '2023-10-01'
)
-- Native SQL PIVOT
select *
from (
    select
        firmware,
        device_code,
        temp_c
    from cleaned
) src
pivot (
    max(temp_c) as total
    for device_code in ('DV-01' as dv_01, 'DV-02' as dv_02, 'DV-03' as dv_03)
);
