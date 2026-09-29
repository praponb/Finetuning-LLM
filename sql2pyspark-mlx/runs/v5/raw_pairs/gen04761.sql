with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        round(humidity, 1) as humidity_rounded
    from device_readings
    where to_date(reading_ts) >= '2025-02-01'
),
kept_keys as (
    -- Set difference on keys
    select firmware
    from cleaned
    except
    select firmware
    from device_readings
    where pressure < 999
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.firmware = k.firmware
),
enriched as (
    -- Ranking and gap features
    select
        *,
        count(temp_c) over w_ordered as device_code_count_temp_c,
        count(humidity) over w_ordered as device_code_count_humidity
    from kept_rows
    window w_ordered as (partition by device_code)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        temp_c,
        humidity,
        status_bits,
        firmware,
        device_code_count_temp_c,
        device_code_count_humidity,
        case when humidity >= 78 then 'High' when humidity >= 30 then 'Medium' else 'Low' end as humidity_level
    from enriched
    where humidity_rounded between 27 and 45
),
kept_keys_2 as (
    -- Set intersection on keys
    select firmware
    from base
    intersect all
    select firmware
    from device_readings
    where status_bits <= 9
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys_2 k on s.firmware = k.firmware
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        humidity - temp_c as humidity_minus_temp_c,
        humidity * status_bits as humidity_times_status_bits
    from verified
    where device_code_count_temp_c is not null
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        temp_c,
        humidity,
        device_code_count_temp_c,
        device_code_count_humidity,
        humidity_level,
        humidity_minus_temp_c,
        percent_rank() over w_running as status_bits_percent_rank,
        rank() over w_running as status_bits_rank,
        dense_rank() over (order by humidity nulls last) as humidity_dense_rank
    from valid
    window w_running as (order by status_bits)
)
-- Multi-level aggregation
select
    case when humidity_minus_temp_c >= 84 then 'LARGE' else 'SMALL' end as humidity_minus_temp_c_tier,
    percentile_approx(status_bits_percent_rank, array(0.25, 0.5, 0.75)) as status_bits_percent_rank_quartiles,
    count(*) as cnt,
    count(*) as row_count,
    round(avg(status_bits_rank), 2) as avg_status_bits_rank,
    round(avg(status_bits_rank), 2) as avg_status_bits_rank_2,
    count(distinct id) as distinct_ids
from metrics
group by 1
having avg_status_bits_rank < 1000000
order by humidity_minus_temp_c_tier desc;
