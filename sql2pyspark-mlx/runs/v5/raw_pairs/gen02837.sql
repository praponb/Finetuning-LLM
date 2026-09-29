with base as (
    -- Keep recent, valid rows
    select *, case when humidity >= 73 then 'High' when humidity >= 44 then 'Medium' else 'Low' end as humidity_level
    from device_readings
    where reading_ts >= timestamp '2025-09-01 02:00:00'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(humidity, 1) over (partition by device_code order by reading_ts, id) as next_humidity,
        sum(status_bits) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_status_bits,
        sum(pressure) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as pressure_sum_15m,
        corr(temp_c, pressure) over w_hist as corr_temp_c_pressure
    from base
    where status_bits = 11
    window w_hist as (partition by device_code)
),
prev_events as (
    -- Previous event time per key
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from enriched
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '30' minute then 1 else 0 end as is_new_session
    from prev_events
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        humidity_level,
        running_status_bits,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        temp_c,
        firmware,
        first_value(running_status_bits, true) over w_grp as first_known_running_status_bits,
        lead(pressure, 1) over w_hist as next_pressure,
        first_value(running_status_bits) ignore nulls over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as first_known_running_status_bits_2,
        last_value(pressure) ignore nulls over (partition by device_code order by pressure, id rows between unbounded preceding and current row) as last_known_pressure
    from sessionized
    window w_grp as (order by burst_id, id rows between unbounded preceding and current row), w_hist as (partition by device_code order by pressure, id)
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        temp_c,
        next_pressure,
        last_known_pressure,
        cast(first_known_running_status_bits_2 as double) as first_known_running_status_bits_2_num
    from with_windows
    where firmware = '1.0.3'
)
select
    id,
    temp_c,
    next_pressure,
    last_known_pressure,
    first_known_running_status_bits_2_num
from valid
order by temp_c desc nulls first, next_pressure desc;
