with with_gaps as (
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '20' minute then 1 else 0 end as is_new_session
    from device_readings
),
islands as (
    select *, sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    burst_id,
    round(avg(is_new_session), 2) as avg_is_new_session,
    min(reading_ts) as started_at,
    avg(temp_c) filter (where device_code = 'DV-02') as dv_02_avg_temp_c
from islands
group by 1
having avg(temp_c) filter (where device_code = 'DV-02') < 1000000
order by dv_02_avg_temp_c;
