with prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) as prev_reading_ts
    from device_readings
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 45 minutes then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    prev_reading_ts,
    is_new_burst,
    session_no
from islands
order by reading_ts asc nulls first, humidity asc;
