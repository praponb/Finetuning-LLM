with metrics as (
    -- Window metrics over several different partitions
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        min(temp_c) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as running_min_temp_c,
        count(temp_c) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as running_count_temp_c,
        max(temp_c) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as running_max_temp_c,
        sum(temp_c) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as running_sum_temp_c,
        sum(pressure) over (partition by device_code order by cast(reading_ts as bigint) range between 3600 preceding and current row) as pressure_sum_1h
    from device_readings
    where temp_c >= 23
)
select
    id,
    reading_ts,
    humidity,
    pressure,
    firmware,
    running_count_temp_c,
    running_max_temp_c,
    running_sum_temp_c
from metrics
order by humidity desc;
