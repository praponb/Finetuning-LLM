with filtered as (
    -- Keep recent, valid rows
    select *, hour(reading_ts) as reading_ts_hour
    from device_readings
    where reading_ts >= timestamp '2025-09-01 05:00:00'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 45 minutes then 1 else 0 end as is_new_session
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        humidity,
        reading_ts_hour,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '1' hour then 1 else 0 end as is_new_session
    from sessionized
),
sessionized_device_readings as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        reading_ts_hour,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 30 minutes then 1 else 0 end as is_new_burst
    from sessionized_device_readings
),
sessionized_device_readings_2 as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from flagged
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by device_code order by reading_ts_hour desc, id) as rn
    from sessionized_device_readings_2
    qualify rn = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        reading_ts,
        reading_ts_hour,
        is_new_burst,
        session_id,
        lead(session_id) over (partition by device_code order by is_new_burst, id) as next_session_id,
        lag(island_id) over (partition by device_code order by is_new_burst, id) as prev_island_id,
        lead(island_id) over (partition by device_code order by is_new_burst, id) as next_island_id,
        row_number() over (partition by device_code order by is_new_burst, id) as row_seq,
        avg(reading_ts_hour) over (partition by device_code order by reading_ts_hour, id rows between 2 preceding and current row) as reading_ts_hour_avg_2
    from ranked
    where device_code not in ('DV-04', 'DV-02')
)
-- Aggregate with conditional metrics
select
    case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
    approx_percentile(reading_ts_hour, 0.95) as reading_ts_hour_p95,
    round(avg(row_seq), 2) as avg_row_seq
from with_windows
group by 1
having round(avg(row_seq), 2) >= 0
order by span_minutes desc
