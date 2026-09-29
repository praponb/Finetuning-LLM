with filtered as (
    -- Basic cleansing and defaults
    select *, pressure + status_bits pressure_plus_status_bits
    from device_readings
    where reading_ts >= to_timestamp('2025-09-01 08:00:00')
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(pressure) over w_grp pressure_avg_30m,
        sum(pressure) over (order by cast(reading_ts as bigint) range between 3600 preceding and current row) pressure_sum_1h,
        count(temp_c) over w_grp temp_c_count_30m,
        row_number() over (partition by device_code order by reading_ts, id) row_seq
    from filtered
    window w_grp as (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row)
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from with_windows
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 15 minutes then 1 else 0 end as new_group_flag
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        pressure_avg_30m,
        pressure_sum_1h,
        temp_c_count_30m,
        sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '20' minute then 1 else 0 end as is_new_burst
    from islands
),
islands_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(pressure_avg_30m) over (partition by device_code order by pressure_avg_30m, id rows between unbounded preceding and current row) running_pressure_avg_30m,
        first_value(temp_c_count_30m, true) over (partition by device_code order by session_id, id rows between unbounded preceding and current row) first_known_temp_c_count_30m,
        first_value(is_new_burst, true) over (order by reading_ts, id rows between unbounded preceding and current row) first_known_is_new_burst,
        row_number() over (partition by device_code order by reading_ts, id) event_seq
    from islands_device_readings
)
select
    id,
    device_code,
    status_bits,
    firmware,
    pressure_sum_1h,
    temp_c_count_30m,
    island_id,
    is_new_burst,
    session_id,
    case when device_code rlike '^(DV-04|DV-01)$' then 'CORE' when device_code rlike '^(DV-02|DV-03)$' then 'GROUP_A' else 'GROUP_B' end as device_code_zone
from metrics
order by session_id desc;
