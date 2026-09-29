with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 20 minutes then 1 else 0 end as new_group_flag
    from device_readings
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    id,
    device_code,
    humidity,
    pressure,
    status_bits,
    firmware,
    new_group_flag,
    session_id
from sessionized
order by device_code nulls first
