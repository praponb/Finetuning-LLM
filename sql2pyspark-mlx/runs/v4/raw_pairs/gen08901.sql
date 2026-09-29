select
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    avg(status_bits) over (order by pressure, id rows between 5 preceding and 1 preceding) as status_bits_avg_5_prior,
    corr(humidity, pressure) over w_grp as corr_humidity_pressure,
    corr(temp_c, pressure) over w_grp as corr_temp_c_pressure
from device_readings
window w_grp as (partition by device_code)
