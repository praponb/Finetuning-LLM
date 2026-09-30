with base as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), reading_ts) as days_since_reading_ts
    from device_readings
    where reading_ts > timestamp '2025-09-01 07:00:00'
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 15 minutes then 1 else 0 end as is_new_burst
    from base
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
-- Aggregate with conditional metrics
select
    island_id,
    date_trunc('hour', reading_ts) as hour_window,
    corr(temp_c, pressure) as corr_temp_c_pressure,
    min(reading_ts) as started_at,
    avg(humidity) as avg_humidity
from islands
group by island_id, date_trunc('hour', reading_ts)
order by avg_humidity desc nulls first
