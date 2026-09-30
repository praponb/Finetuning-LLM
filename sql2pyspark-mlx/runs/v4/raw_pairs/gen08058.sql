with with_windows as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        avg(humidity) over (order by status_bits, id rows between 3 preceding and current row) as humidity_avg_3,
        stddev_samp(pressure) over (partition by device_code) as device_code_stddev_samp_pressure
    from device_readings
)
select
    id,
    reading_ts,
    humidity,
    pressure,
    firmware,
    device_code_stddev_samp_pressure
from with_windows
order by id desc nulls last
limit 20
