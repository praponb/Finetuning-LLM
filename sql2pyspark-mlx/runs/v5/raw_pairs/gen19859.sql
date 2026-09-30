with filtered as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        pressure - temp_c as pressure_minus_temp_c,
        temp_c * pressure as temp_c_times_pressure
    from device_readings
    where hour(reading_ts) between 6 and 14
),
flagged as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) > interval '30' minute then 1 else 0 end as session_start_flag
    from filtered
),
sessions_device_readings as (
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    date_trunc('day', reading_ts) as activity_day,
    approx_percentile(temp_c, 0.5) as temp_c_p50,
    count(distinct firmware) as distinct_firmwares,
    avg(status_bits) as avg_status_bits,
    max(reading_ts) as last_seen,
    count_if(device_code = 'DV-04') as dv_04_events
from sessions_device_readings
group by date_trunc('day', reading_ts)
order by activity_day desc nulls last;
