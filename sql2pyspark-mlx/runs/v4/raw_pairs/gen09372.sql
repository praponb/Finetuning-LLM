with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from device_readings
    qualify row_number() over (partition by device_code order by pressure desc, id asc) = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) > interval 15 minutes then 1 else 0 end as is_new_session
    from deduped
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as session_id
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_new_session,
        session_id,
        row_number() over (partition by device_code order by status_bits asc, id asc) as row_seq,
        avg(is_new_session) over (partition by device_code order by is_new_session asc, id asc rows between 6 preceding and current row) as is_new_session_avg_6,
        avg(humidity) over (partition by device_code order by reading_ts asc, id asc rows between 5 preceding and 1 preceding) as humidity_avg_5_prior,
        corr(pressure, status_bits) over (partition by device_code) as corr_pressure_status_bits,
        max(is_new_session) over (partition by device_code order by cast(reading_ts as bigint) asc range between 3600 preceding and current row) as is_new_session_max_1h
    from islands
),
kept_keys as (
    -- Set difference on keys
    select firmware, id
    from enriched
    except
    select firmware, id
    from device_readings
    where hour(reading_ts) between 7 and 13
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.firmware = k.firmware and s.id = k.id
),
enriched_device_readings as (
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
        is_new_session,
        session_id,
        row_seq,
        is_new_session_avg_6,
        humidity_avg_5_prior,
        corr_pressure_status_bits,
        is_new_session_max_1h,
        round(humidity / sum(humidity) over (partition by device_code), 4) as humidity_share,
        rank() over (partition by device_code order by temp_c asc nulls last) as temp_c_rank,
        count(row_seq) over (partition by device_code order by cast(reading_ts as bigint) asc range between 7200 preceding and current row) as row_seq_count_2h,
        rank() over (partition by device_code order by corr_pressure_status_bits asc nulls first) as corr_pressure_status_bits_rank,
        count(is_new_session_max_1h) over (partition by device_code) as device_code_count_is_new_session_max_1h
    from verified
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        is_new_session,
        session_id,
        row_seq,
        is_new_session_avg_6,
        humidity_avg_5_prior,
        corr_pressure_status_bits,
        is_new_session_max_1h,
        temp_c_rank,
        row_seq_count_2h,
        device_code_count_is_new_session_max_1h,
        lower(firmware) as firmware_lower
    from enriched_device_readings
    where reading_ts >= date '2025-09-01'
      and humidity_avg_5_prior <> 54
),
stats as (
    -- Multi-level aggregation
    select
        case when is_new_session_max_1h >= 1 then 'LARGE' else 'SMALL' end as is_new_session_max_1h_tier,
        avg(is_new_session_avg_6) as avg_is_new_session_avg_6,
        count_if(device_code = 'DV-02') as dv_02_events,
        max(status_bits) filter (where device_code = 'DV-03') as dv_03_max_status_bits
    from cleaned
    group by is_new_session_max_1h_tier
)
select
    is_new_session_max_1h_tier,
    avg_is_new_session_avg_6,
    dv_02_events,
    dv_03_max_status_bits
from stats
order by avg_is_new_session_avg_6 desc
