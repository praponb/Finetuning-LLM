with valid as (
    -- Basic cleansing and defaults
    select *, status_bits * temp_c status_bits_times_temp_c
    from device_readings
    where reading_ts >= timestamp '2025-09-01 15:00:00'
      and pressure between 1004 and 1012
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 15 minutes then 1 else 0 end as new_group_flag
    from valid
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by device_code order by reading_ts, id) as latest_rank
        from sessionized
    ) x
    where latest_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        device_code,
        temp_c,
        status_bits_times_temp_c,
        humidity,
        pressure
    from latest
) src
unpivot include nulls (
    value for kpi in (temp_c, status_bits_times_temp_c, humidity, pressure)
);
