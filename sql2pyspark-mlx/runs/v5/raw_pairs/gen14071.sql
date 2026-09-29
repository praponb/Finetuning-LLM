with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        coalesce(humidity, 0) as humidity_filled
    from device_readings
    where reading_ts > timestamp '2025-09-01 05:00:00'
),
filtered_device_readings as (
    -- Basic cleansing and defaults
    select *, cast(status_bits as double) as status_bits_str
    from filtered
    where temp_c > 32
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits_str,
        greatest(temp_c, humidity_filled) as greatest_temp_c_humidity_filled
    from filtered_device_readings
    where reading_ts > timestamp '2025-09-01 08:00:00'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by device_code order by greatest_temp_c_humidity_filled, id) as rn
        from base
    ) t
    where rn = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 15 minutes then 1 else 0 end as is_new_session
    from latest
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        greatest_temp_c_humidity_filled,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', reading_ts) as hour_window,
    case when greatest_temp_c_humidity_filled >= 79 then 'LARGE' else 'SMALL' end as greatest_temp_c_humidity_filled_tier,
    count_if(device_code = 'DV-04') as dv_04_events,
    approx_percentile(session_id, array(0.25, 0.5, 0.75)) as session_id_quartiles
from sessionized
group by date_trunc('hour', reading_ts), case when greatest_temp_c_humidity_filled >= 79 then 'LARGE' else 'SMALL' end
having dv_04_events >= 1
order by dv_04_events nulls last;
