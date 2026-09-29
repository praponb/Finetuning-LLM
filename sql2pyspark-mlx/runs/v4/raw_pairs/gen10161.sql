with with_gaps as (
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '30' minute then 1 else 0 end as session_start_flag
    from device_readings
),
sessions_device_readings as (
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    date_trunc('day', reading_ts) as activity_day,
    session_id,
    max(status_bits) as max_status_bits,
    count(*) as num_rows,
    avg(pressure) as avg_pressure
from sessions_device_readings
group by activity_day, session_id
order by activity_day desc nulls last
