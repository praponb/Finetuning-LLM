with enriched as (
    -- Ranking and gap features
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        count(humidity) over (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row) humidity_count_2h,
        sum(pressure) over (order by cast(reading_ts as bigint) range between 900 preceding and current row) pressure_sum_15m
    from device_readings
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over (partition by device_code order by temp_c, id) temp_c_bucket,
        avg(pressure) over (partition by device_code order by pressure_sum_15m, id rows between 6 preceding and 1 preceding) pressure_avg_6_prior,
        avg(temp_c) over w_recent temp_c_avg_2_prior,
        round(pressure_sum_15m / sum(pressure_sum_15m) over (partition by device_code), 4) pressure_sum_15m_share
    from enriched
    window w_recent as (partition by device_code order by temp_c, id rows between 2 preceding and 1 preceding)
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 1 hour then 1 else 0 end as is_new_burst
    from scored
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 30 minutes then 1 else 0 end as is_new_session
    from islands
),
islands_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id_2
    from flagged
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(temp_c_avg_2_prior) over (order by temp_c_bucket, id rows between unbounded preceding and current row) running_temp_c_avg_2_prior,
        sum(session_id_2) over (partition by device_code order by session_id_2, id rows between unbounded preceding and current row) running_session_id_2,
        avg(pressure) over (order by session_id_2, id rows between 3 preceding and 1 preceding) pressure_avg_3_prior
    from islands_device_readings
),
prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from with_windows
),
with_gaps_2 as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '30' minute then 1 else 0 end as session_start_flag
    from prev_events
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from with_gaps_2
),
with_windows_device_readings as (
    -- Running and moving aggregates
    select
        *,
        min(temp_c) over (partition by device_code) device_code_min_temp_c,
        max(session_id_2) over (partition by device_code) device_code_max_session_id_2
    from sessions_device_readings
)
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
    humidity_count_2h,
    pressure_sum_15m,
    temp_c_bucket,
    pressure_avg_6_prior,
    temp_c_avg_2_prior,
    pressure_sum_15m_share,
    is_new_burst,
    session_id,
    is_new_session,
    session_id_2,
    running_temp_c_avg_2_prior,
    running_session_id_2,
    pressure_avg_3_prior,
    prev_reading_ts,
    session_start_flag,
    burst_id,
    device_code_min_temp_c,
    device_code_max_session_id_2,
    max(pressure_avg_6_prior) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) pressure_avg_6_prior_max_30m,
    stddev_samp(humidity_count_2h) over (partition by device_code) device_code_stddev_samp_humidity_count_2h,
    last_value(humidity_count_2h) ignore nulls over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) last_known_humidity_count_2h,
    percent_rank() over (order by device_code_max_session_id_2 desc) device_code_max_session_id_2_percent_rank,
    ntile(4) over (partition by device_code order by running_session_id_2 desc, id) running_session_id_2_bucket
from with_windows_device_readings
