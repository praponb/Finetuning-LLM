with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 1 hour then 1 else 0 end as new_group_flag
    from device_readings
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from flagged
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '15' minute then 1 else 0 end as new_group_flag
    from sessions_device_readings
),
sessions_device_readings_2 as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from sessions_device_readings_2
    qualify row_number() over (partition by device_code order by session_id, id) = 1
),
with_gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 15 minutes then 1 else 0 end as is_new_burst
    from deduped
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        firmware,
        sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from with_gaps_2
)
-- Multi-level aggregation
select
    date_trunc('hour', reading_ts) as hour_window,
    min(reading_ts) as started_at,
    count_if(device_code = 'DV-02') as dv_02_events,
    percentile_approx(temp_c, array(0.25, 0.5, 0.75)) as temp_c_quartiles
from sessionized
group by hour_window
order by dv_02_events
