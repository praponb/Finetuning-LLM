with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 30 minutes then 1 else 0 end as is_new_burst
    from device_readings
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_prev as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from islands
),
gaps_2 as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '15' minute then 1 else 0 end as is_new_session
    from with_prev
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps_2
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        pressure,
        is_new_burst,
        burst_id,
        prev_reading_ts,
        is_new_session,
        island_id
    from (
        select *, row_number() over (partition by device_code order by is_new_session desc, id) as rank_in_group
        from sessions_device_readings
    ) t
    where rank_in_group = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        prev_reading_ts,
        island_id,
        lag(pressure) over (order by pressure, id) as prev_pressure,
        lead(pressure) over (order by pressure, id) as next_pressure,
        lead(is_new_burst) over (order by pressure, id) as next_is_new_burst,
        row_number() over (order by pressure, id) as seq_no
    from ranked
),
enriched_device_readings as (
    -- Ranking and gap features
    select
        id,
        seq_no,
        max(prev_pressure) over (order by cast(prev_reading_ts as bigint) range between 7776000 preceding and current row) as prev_pressure_max_90d,
        sum(seq_no) over (order by cast(prev_reading_ts as bigint) range between 2592000 preceding and current row) as seq_no_sum_30d,
        lag(seq_no, 1) over (order by prev_reading_ts, id) as prev_seq_no,
        last_value(prev_pressure) ignore nulls over (order by seq_no, id rows between unbounded preceding and current row) as last_known_prev_pressure,
        count(next_is_new_burst) over (order by cast(prev_reading_ts as bigint) range between 2592000 preceding and current row) as next_is_new_burst_count_30d
    from enriched
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        seq_no,
        prev_pressure_max_90d,
        prev_seq_no,
        next_is_new_burst_count_30d,
        lead(last_known_prev_pressure) over (order by next_is_new_burst_count_30d, id) as next_last_known_prev_pressure,
        lag(last_known_prev_pressure) over (order by next_is_new_burst_count_30d, id) as prev_last_known_prev_pressure,
        lead(next_is_new_burst_count_30d) over (order by next_is_new_burst_count_30d, id) as next_next_is_new_burst_count_30d,
        row_number() over (order by next_is_new_burst_count_30d, id) as event_seq
    from enriched_device_readings
    where prev_pressure_max_90d < 1000
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, floor(prev_pressure_max_90d) as prev_pressure_max_90d_whole
    from metrics
    where prev_last_known_prev_pressure > 1004
)
select
    id,
    seq_no,
    prev_seq_no,
    next_last_known_prev_pressure,
    prev_last_known_prev_pressure,
    next_next_is_new_burst_count_30d,
    prev_pressure_max_90d_whole
from base
order by prev_last_known_prev_pressure desc
