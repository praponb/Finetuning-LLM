select
    *,
    lead(temp_c, 1, 0) over (partition by device_code order by temp_c, id) as next_temp_c,
    max(humidity) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as humidity_max_15m
from device_readings;
