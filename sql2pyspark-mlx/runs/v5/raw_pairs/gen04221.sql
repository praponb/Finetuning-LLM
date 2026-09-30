with filtered as (
    -- Keep recent, valid rows
    select *, least(pressure, status_bits) as least_pressure_status_bits
    from device_readings
    where reading_ts >= to_timestamp('2025-09-01 08:00:00')
),
scored as (
    -- Ranking and gap features
    select
        *,
        lead(temp_c) over (partition by device_code order by pressure, id) as next_temp_c,
        lag(temp_c) over (partition by device_code order by pressure, id) as prev_temp_c,
        lead(pressure) over (partition by device_code order by pressure, id) as next_pressure,
        row_number() over (partition by device_code order by pressure, id) as row_seq,
        row_number() over (partition by device_code order by humidity desc, id) as event_seq
    from filtered
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by device_code order by next_temp_c desc nulls first, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(next_pressure) ignore nulls over (partition by device_code order by event_seq, id rows between unbounded preceding and current row) as last_known_next_pressure,
        last_value(event_seq) ignore nulls over (partition by device_code order by row_seq, id rows between unbounded preceding and current row) as last_known_event_seq,
        cume_dist() over (partition by device_code order by temp_c nulls last) as temp_c_cume_dist,
        row_number() over (partition by device_code order by row_seq desc, id) as row_seq_2
    from deduped
),
prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from enriched
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 45 minutes then 1 else 0 end as is_new_session
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        least_pressure_status_bits,
        prev_temp_c,
        event_seq,
        last_known_next_pressure,
        last_known_event_seq,
        temp_c_cume_dist,
        row_seq_2,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from islands
),
gaps_2 as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '1' hour then 1 else 0 end as session_start_flag
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_no
    from gaps_2
)
-- Native SQL PIVOT
select *
from (
    select
        firmware,
        device_code,
        prev_temp_c
    from sessionized
) src
pivot (
    sum(prev_temp_c) as amount,
    count(1) as cnt
    for device_code in ('DV-02' as dv_02, 'DV-04' as dv_04)
)
