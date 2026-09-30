with metrics as (
    select
        *,
        lag(temp_c) over (partition by device_code order by pressure, id) as prev_temp_c,
        lead(temp_c) over (partition by device_code order by pressure, id) as next_temp_c,
        lead(humidity) over (partition by device_code order by pressure, id) as next_humidity,
        row_number() over (partition by device_code order by pressure, id) as seq_no,
        last_value(pressure, true) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as last_known_pressure
    from device_readings
),
latest as (
    select
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        next_temp_c,
        next_humidity,
        seq_no
    from metrics
    qualify row_number() over (partition by device_code order by status_bits desc, id) = 1
),
with_windows as (
    select
        *,
        count(pressure) over (order by status_bits, id rows between unbounded preceding and current row) as running_count_pressure,
        min(pressure) over (order by status_bits, id rows between unbounded preceding and current row) as running_min_pressure,
        max(pressure) over (order by status_bits, id rows between unbounded preceding and current row) as running_max_pressure,
        sum(pressure) over (order by status_bits, id rows between unbounded preceding and current row) as running_sum_pressure
    from latest
    where pressure < 1009
),
valid as (
    select
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        next_temp_c,
        seq_no,
        running_max_pressure,
        running_sum_pressure,
        cast(running_min_pressure as int) as running_min_pressure_int
    from with_windows
    where running_count_pressure >= 16
),
valid_device_readings as (
    select
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        next_temp_c,
        seq_no,
        running_max_pressure,
        running_min_pressure_int,
        round(pressure / running_sum_pressure, 2) as pressure_per_running_sum_pressure,
        coalesce(seq_no, 0) as seq_no_filled
    from valid
    where status_bits between 8 and 10
),
filtered as (
    select
        id,
        temp_c,
        pressure,
        status_bits,
        next_temp_c,
        seq_no,
        running_max_pressure,
        running_min_pressure_int,
        pressure_per_running_sum_pressure * seq_no as pressure_per_running_sum_pressure_times_seq_no,
        greatest(next_temp_c, temp_c) as greatest_next_temp_c_temp_c
    from valid_device_readings
    where temp_c <= 31
),
enriched as (
    select
        *,
        row_number() over (order by pressure_per_running_sum_pressure_times_seq_no, id) as event_seq,
        ntile(3) over w_recent as running_min_pressure_int_bucket
    from filtered
    window w_recent as (order by running_min_pressure_int, id)
)
select
    id,
    temp_c,
    pressure,
    next_temp_c,
    seq_no,
    running_min_pressure_int,
    pressure_per_running_sum_pressure_times_seq_no,
    greatest_next_temp_c_temp_c,
    event_seq
from enriched
order by id
limit 20;
