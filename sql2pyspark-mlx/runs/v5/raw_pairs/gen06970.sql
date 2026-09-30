with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 1 hour then 1 else 0 end as is_new_session
    from device_readings
)
-- Running sum of the flag = session number
select
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
from gaps;
