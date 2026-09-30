with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when device_code = 'DV-04' then 1 else 0 end as is_dv_04
    from device_readings
    where reading_ts > to_date('2025-09-01')
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by temp_c desc nulls first, id) as latest_rank
    from cleaned
    qualify latest_rank = 1
),
ranked_device_readings as (
    -- Deduplicate with ROW_NUMBER
    select *
    from ranked
    qualify rank() over (partition by device_code order by humidity) = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(is_dv_04, 1) over (order by pressure, id) as next_is_dv_04,
        lead(pressure, 2) over (order by temp_c, id) as next_pressure,
        row_number() over (order by reading_ts, id) as row_seq,
        row_number() over (partition by device_code order by status_bits desc, id) as event_seq
    from ranked_device_readings
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, round(row_seq / temp_c, 2) as row_seq_per_temp_c
    from scored
    where reading_ts >= to_date('2025-09-01')
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from filtered
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '1' hour then 1 else 0 end as is_new_burst
    from with_prev
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
remaining_keys as (
    -- Set difference on keys
    select firmware, device_code
    from sessions_device_readings
    except distinct
    select firmware, device_code
    from device_readings
    where firmware <> '1.0.3'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_device_readings s
    join remaining_keys k on s.firmware = k.firmware and s.device_code = k.device_code
)
select
    reading_ts,
    temp_c,
    humidity,
    pressure,
    firmware,
    is_dv_04,
    next_is_dv_04,
    next_pressure,
    row_seq
from verified
