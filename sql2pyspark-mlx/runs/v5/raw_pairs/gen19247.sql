with metrics as (
    select
        *,
        last_value(humidity) ignore nulls over (order by humidity, id rows between unbounded preceding and current row) as last_known_humidity,
        sum(temp_c) over (partition by device_code order by status_bits, id rows between unbounded preceding and current row) as running_temp_c,
        row_number() over (partition by device_code order by humidity, id) as row_seq,
        row_number() over (partition by device_code order by temp_c, id) as seq_no
    from device_readings
)
select
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    firmware,
    last_known_humidity,
    running_temp_c
from metrics
order by humidity desc nulls last;
