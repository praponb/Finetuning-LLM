with base as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        case when temp_c >= 37.5 then 'High' when temp_c >= 26.3 then 'Medium' else 'Low' end as temp_c_tier
    from device_readings
    where reading_ts > date '2025-09-01'
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '15' minute then 1 else 0 end as new_group_flag
    from base
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by device_code order by new_group_flag desc nulls last, id) as rn
    from sessions_device_readings
    qualify rn = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        device_code,
        temp_c,
        pressure,
        firmware,
        temp_c_tier,
        max(new_group_flag) over w_recent as running_max_new_group_flag,
        sum(new_group_flag) over w_recent as running_sum_new_group_flag,
        avg(new_group_flag) over w_recent as running_avg_new_group_flag
    from deduped
    window w_recent as (partition by device_code order by status_bits, id rows between unbounded preceding and current row)
)
select
    id,
    device_code,
    firmware,
    running_max_new_group_flag,
    running_avg_new_group_flag
from with_windows
order by device_code desc nulls first;
