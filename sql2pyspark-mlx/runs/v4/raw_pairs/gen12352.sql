with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) + interval '20' minute then 1 else 0 end as is_new_burst
    from device_readings
)
-- Running sum of the flag = session number
select *, sum(is_new_burst) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as session_no
from flagged
