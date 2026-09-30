with prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from device_readings
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 15 minutes then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_reading_ts,
        is_new_burst,
        burst_id,
        last_value(is_new_burst, true) over w_grp as last_known_is_new_burst,
        first_value(pressure) ignore nulls over (partition by device_code order by status_bits, id rows between unbounded preceding and current row) as first_known_pressure,
        max(status_bits) over (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row) as status_bits_max_2h
    from islands
    window w_grp as (partition by device_code order by burst_id, id rows between unbounded preceding and current row)
),
enriched as (
    -- Ranking and gap features
    select
        *,
        count(is_new_burst) over (partition by device_code order by cast(reading_ts as bigint) range between 3600 preceding and current row) as is_new_burst_count_1h,
        avg(burst_id) over (order by cast(reading_ts as bigint) range between 7200 preceding and current row) as burst_id_avg_2h,
        lag(humidity, 1, 0) over (partition by device_code order by humidity, id) as prev_humidity,
        stddev_samp(pressure) over (partition by device_code) as device_code_stddev_samp_pressure,
        max(last_known_is_new_burst) over (partition by device_code order by cast(prev_reading_ts as bigint) range between 7776000 preceding and current row) as last_known_is_new_burst_max_90d
    from metrics
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_reading_ts,
        burst_id,
        last_known_is_new_burst,
        status_bits_max_2h,
        is_new_burst_count_1h,
        burst_id_avg_2h,
        prev_humidity,
        case when firmware = '1.1.0' then 1 else 0 end as is_1_1_0
    from enriched
    where prev_humidity <= 73
),
filtered_device_readings as (
    -- Keep recent, valid rows
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        burst_id,
        status_bits_max_2h,
        is_new_burst_count_1h,
        burst_id_avg_2h,
        prev_humidity,
        is_1_1_0,
        status_bits * humidity as status_bits_times_humidity
    from filtered
    where temp_c between 35 and 38
),
filtered_device_readings_2 as (
    -- Filter the raw rows and derive helper columns
    select *, round(is_1_1_0, 0) as is_1_1_0_rounded
    from filtered_device_readings
    where reading_ts >= to_date('2025-09-01')
      and pressure < 1023
),
cleaned as (
    -- Basic cleansing and defaults
    select *, ceil(is_1_1_0) as is_1_1_0_whole
    from filtered_device_readings_2
    where reading_ts > to_date('2025-09-01')
)
-- Ranking and gap features
select
    *,
    first_value(is_1_1_0, true) over (order by status_bits, id rows between unbounded preceding and current row) as first_known_is_1_1_0,
    first_value(is_1_1_0_rounded, true) over w_running as first_known_is_1_1_0_rounded,
    ntile(3) over w_ordered as prev_humidity_bucket
from cleaned
window w_running as (order by humidity, id rows between unbounded preceding and current row), w_ordered as (order by prev_humidity desc, id);
