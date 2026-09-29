with cleaned as (
    -- Keep recent, valid rows
    select *, ceil(humidity) as humidity_whole
    from device_readings
    where reading_ts > date '2025-09-01'
      and device_code in ('DV-02', 'DV-04', 'DV-03')
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 45 minutes then 1 else 0 end as session_start_flag
    from cleaned
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
)
-- Aggregate with conditional metrics
select
    case when island_id >= 7 then 'LARGE' else 'SMALL' end as island_id_tier,
    corr(temp_c, pressure) as corr_temp_c_pressure,
    count(distinct id) as distinct_ids,
    count_if(device_code = 'DV-02') as dv_02_events,
    size(collect_set(device_code)) as num_device_codes
from islands
group by 1
order by island_id_tier nulls last;
