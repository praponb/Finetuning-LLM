with valid as (
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
        datediff(current_date(), reading_ts) as days_since_reading_ts
    from device_readings
    where status_bits > 8
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by device_code order by status_bits desc, id asc) as row_num
    from valid
    qualify row_num = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) + interval '1' hour then 1 else 0 end as is_new_session
    from latest
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from sessions_device_readings
    qualify rank() over (partition by device_code order by status_bits asc) = 1
),
scored as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        temp_c,
        humidity,
        status_bits,
        firmware,
        is_new_session,
        island_id,
        row_number() over (order by island_id desc, id asc) as row_seq,
        round(temp_c / sum(temp_c) over (partition by device_code), 4) as temp_c_share,
        sum(pressure) over (partition by device_code order by pressure asc, id asc rows between unbounded preceding and current row) as cumulative_pressure
    from deduped
)
select
    id,
    device_code,
    temp_c,
    humidity,
    status_bits,
    firmware,
    island_id,
    temp_c_share,
    cumulative_pressure,
    case when device_code rlike '^(DV-01|DV-04)$' then 'PRIORITY' when device_code rlike '^(DV-02|DV-03)$' then 'OTHER_ZONE' else 'CORE' end as device_code_zone
from scored
order by device_code desc nulls first, humidity asc;
