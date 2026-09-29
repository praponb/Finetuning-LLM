select
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    sum(humidity) over (partition by device_code order by cast(reading_ts as bigint) asc range between 3600 preceding and current row) as humidity_sum_1h,
    max(temp_c) over (partition by device_code) as device_code_max_temp_c
from device_readings;
