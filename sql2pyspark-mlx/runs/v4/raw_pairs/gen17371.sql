select *
from (
    select
        id,
        device_code,
        humidity,
        temp_c
    from device_readings
) src
unpivot include nulls (
    value for metric in (humidity, temp_c)
);
