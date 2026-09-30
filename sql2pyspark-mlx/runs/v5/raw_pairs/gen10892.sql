with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '45' minute then 1 else 0 end as session_start_flag
    from device_readings
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', reading_ts) as hour_window,
    device_code,
    size(collect_set(device_code)) as num_device_codes,
    sort_array(collect_set(device_code)) as device_code_list,
    count(distinct firmware) as distinct_firmwares,
    count(case when device_code = 'DV-02' then 1 end) as dv_02_count,
    stddev_samp(pressure) as pressure_stddev,
    min(reading_ts) as first_reading_ts
from sessionized
group by 1, 2
order by distinct_firmwares nulls last;
