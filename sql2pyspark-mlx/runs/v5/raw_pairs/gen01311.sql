with filtered as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits + pressure as status_bits_plus_pressure,
        round(temp_c * 100, 2) as adjusted_temp_c
    from device_readings
    where temp_c > 38
),
gaps as (
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 45 minutes then 1 else 0 end as is_new_burst
    from filtered
),
sessions_device_readings as (
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from gaps
)
select
    date_trunc('day', reading_ts) as activity_day,
    count(*) as event_count,
    count(case when device_code = 'DV-04' then 1 end) as dv_04_count,
    sum(session_no) filter (where device_code = 'DV-04') as dv_04_total_session_no
from sessions_device_readings
group by date_trunc('day', reading_ts)
order by activity_day
