with filtered as (
    -- Keep recent, valid rows
    select
        *,
        round(status_bits, 2) as status_bits_rounded,
        round(pressure, 1) as pressure_rounded
    from device_readings
    where reading_ts >= timestamp '2025-09-01 15:00:00'
),
kept_keys as (
    -- Set difference on keys
    select firmware
    from filtered
    except
    select firmware
    from device_readings
    where pressure < 1012
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.firmware = k.firmware
),
enriched as (
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
        status_bits_rounded,
        pressure_rounded,
        max(temp_c) over w_part as device_code_max_temp_c,
        corr(humidity, status_bits) over w_part as corr_humidity_status_bits,
        corr(pressure, status_bits) over w_part as corr_pressure_status_bits
    from kept_rows
    window w_part as (partition by device_code)
),
stats as (
    -- Aggregate with conditional metrics
    select
        case when pressure_rounded >= 85 then 'PREMIUM' else 'STANDARD' end as pressure_rounded_tier,
        (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as duration_min,
        (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as span_minutes,
        count(*) as event_count,
        count(distinct id) as distinct_ids
    from enriched
    group by 1
)
select
    pressure_rounded_tier,
    duration_min,
    event_count,
    distinct_ids
from stats
order by event_count asc nulls first;
