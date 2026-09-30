with cleaned as (
    -- Keep recent, valid rows
    select *, round(humidity * 1.1, 2) as humidity_adjusted
    from device_readings
    where reading_ts > timestamp '2025-09-01 06:00:00'
      and firmware in ('1.0.3', '1.1.0')
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        device_code,
        humidity_adjusted,
        pressure,
        humidity,
        temp_c
    from cleaned
) src
unpivot (
    val for measure in (humidity_adjusted, pressure, humidity, temp_c)
) long_rows;
