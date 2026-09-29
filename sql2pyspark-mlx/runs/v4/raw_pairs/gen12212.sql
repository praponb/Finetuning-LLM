with valid as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        case when status_bits >= 13 then 'High' when status_bits >= 4 then 'Medium' else 'Low' end as status_bits_tier
    from device_readings
    where to_date(reading_ts) < '2025-09-07'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by device_code order by pressure nulls first) as pressure_percent_rank,
        count(pressure) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as pressure_count_15m,
        count(pressure) over (partition by device_code) as device_code_count_pressure,
        first_value(temp_c, true) over (order by temp_c, id rows between unbounded preceding and current row) as first_known_temp_c
    from valid
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        status_bits_tier,
        pressure_percent_rank,
        pressure_count_15m,
        device_code_count_pressure,
        first_known_temp_c,
        avg(first_known_temp_c) over (partition by device_code order by pressure, id rows between 3 preceding and current row) as first_known_temp_c_avg_3,
        avg(pressure_percent_rank) over (partition by device_code order by temp_c, id rows between 3 preceding and 1 preceding) as pressure_percent_rank_avg_3_prior,
        row_number() over w_part as row_seq,
        last_value(pressure_percent_rank, true) over (partition by device_code order by status_bits, id rows between unbounded preceding and current row) as last_known_pressure_percent_rank,
        sum(pressure) over (order by pressure, id rows between unbounded preceding and current row) as running_pressure
    from with_windows
    window w_part as (partition by device_code order by pressure_count_15m, id)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        status_bits_tier,
        pressure_percent_rank,
        pressure_count_15m,
        device_code_count_pressure,
        first_known_temp_c,
        first_known_temp_c_avg_3,
        pressure_percent_rank_avg_3_prior,
        last_known_pressure_percent_rank,
        running_pressure,
        date_format(reading_ts, 'yyyy-MM') as reading_ts_label
    from scored
    where reading_ts >= date '2025-09-01'
      and device_code_count_pressure between 5 and 13
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval '15' minute then 1 else 0 end as new_group_flag
    from filtered
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as session_id
    from flagged
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval '45' minute then 1 else 0 end as is_new_burst
    from sessions_device_readings
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
),
filtered_device_readings as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        status_bits_tier,
        pressure_percent_rank,
        pressure_count_15m,
        device_code_count_pressure,
        first_known_temp_c_avg_3,
        last_known_pressure_percent_rank,
        running_pressure,
        reading_ts_label,
        new_group_flag,
        session_id,
        is_new_burst,
        least(device_code_count_pressure, session_id) as least_device_code_count_pressure_session_id
    from islands
    where pressure <= 1001
),
daily as (
    -- Multi-level aggregation
    select
        device_code,
        min(reading_ts) as first_reading_ts,
        corr(pressure, status_bits) as corr_pressure_status_bits,
        (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes
    from filtered_device_readings
    group by device_code
)
select
    device_code,
    first_reading_ts,
    corr_pressure_status_bits,
    span_minutes
from daily
order by first_reading_ts desc;
