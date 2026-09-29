with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        humidity,
        pressure,
        firmware,
        least(pressure, humidity) as least_pressure_humidity
    from device_readings
    where reading_ts >= to_timestamp('2025-09-01 05:00:00')
      and device_code in ('DV-02', 'DV-01', 'DV-04')
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by device_code order by humidity desc nulls first) as humidity_percent_rank,
        lead(pressure, 1) over (partition by device_code order by humidity, id) as next_pressure,
        lead(humidity, 2) over (partition by device_code order by humidity, id) as next_humidity,
        avg(pressure) over (order by least_pressure_humidity, id rows between 6 preceding and current row) as pressure_avg_6
    from filtered
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by device_code order by humidity_percent_rank desc, id) as seq_no,
        first_value(humidity_percent_rank) ignore nulls over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as first_known_humidity_percent_rank
    from enriched
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by seq_no, id) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
),
with_windows_device_readings as (
    -- Window metrics over several different partitions
    select
        *,
        lead(seq_no) over w_running as next_seq_no,
        lead(pressure) over w_running as next_pressure_2,
        lag(pressure) over w_running as prev_pressure,
        row_number() over w_running as event_seq
    from latest
    window w_running as (partition by device_code order by next_pressure, id)
),
latest_device_readings as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by humidity_percent_rank desc, id) as dedup_rank_2
    from with_windows_device_readings
    qualify dedup_rank_2 = 1
)
select
    id,
    device_code,
    pressure,
    firmware,
    least_pressure_humidity,
    humidity_percent_rank,
    next_pressure,
    next_humidity,
    pressure_avg_6
from latest_device_readings
order by next_humidity desc, next_pressure;
