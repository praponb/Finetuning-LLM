with filtered as (
    -- Basic cleansing and defaults
    select *, round(status_bits * 1.15, 2) as status_bits_scaled
    from device_readings
    where reading_ts > to_date('2025-09-01')
      and firmware = '1.1.0'
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '45' minute then 1 else 0 end as is_new_burst
    from filtered
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
filtered_device_readings as (
    -- Basic cleansing and defaults
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_scaled,
        is_new_burst,
        burst_id,
        burst_id * is_new_burst as burst_id_times_is_new_burst,
        round(temp_c * 1.1, 2) as temp_c_adjusted
    from sessions_device_readings
    where humidity between 46 and 50
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_new_burst,
        burst_id,
        burst_id_times_is_new_burst,
        pressure / nullif(burst_id_times_is_new_burst, 0) as pressure_per_burst_id_times_is_new_burst
    from filtered_device_readings
    where reading_ts >= to_timestamp('2025-09-01 08:00:00')
)
-- Multi-level aggregation
select
    burst_id,
    bit_and(status_bits) as status_bits_bit_and,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
    corr(humidity, status_bits) as corr_humidity_status_bits,
    min(reading_ts) as first_seen,
    bit_and(status_bits) as status_bits_bit_and_2,
    round(avg(burst_id), 2) as avg_burst_id
from cleaned
group by 1
