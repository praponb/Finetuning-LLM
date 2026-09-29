select
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    cume_dist() over (partition by device_code order by pressure asc nulls first) as pressure_cume_dist,
    lag(status_bits, 1, 0) over (order by reading_ts asc, id asc) as prev_status_bits,
    avg(humidity) over (partition by device_code) as device_code_avg_humidity
from device_readings;
