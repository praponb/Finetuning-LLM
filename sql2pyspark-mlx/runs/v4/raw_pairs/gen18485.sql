with scored as (
    -- Ranking and gap features
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        cume_dist() over (partition by device_code order by temp_c desc) as temp_c_cume_dist,
        row_number() over (partition by device_code order by temp_c desc, id) as event_seq
    from device_readings
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '45' minute then 1 else 0 end as session_start_flag
    from scored
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from islands
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '1' hour then 1 else 0 end as is_new_burst
    from with_prev
),
islands_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id_2
    from with_gaps
),
enriched as (
    -- Ranking and gap features
    select
        *,
        sum(event_seq) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_event_seq,
        row_number() over w_grp as row_seq,
        row_number() over (partition by device_code order by burst_id_2 desc, id) as seq_no,
        sum(event_seq) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as cumulative_event_seq
    from islands_device_readings
    window w_grp as (partition by device_code order by humidity desc, id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_cume_dist,
        event_seq,
        burst_id,
        is_new_burst,
        row_seq,
        least(is_new_burst, running_event_seq) as least_is_new_burst_running_event_seq
    from enriched
    where reading_ts >= timestamp '2025-09-01 11:00:00'
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            firmware,
            device_code,
            burst_id
        from base
    ) src
    pivot (
        avg(burst_id) as value_sum
        for device_code in ('DV-04' as dv_04, 'DV-03' as dv_03, 'DV-02' as dv_02)
    )
)
select
    firmware,
    dv_04,
    dv_03
from wide
order by dv_03 desc;
