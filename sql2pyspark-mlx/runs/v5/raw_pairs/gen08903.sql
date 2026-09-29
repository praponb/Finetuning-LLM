with base as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        greatest(status_bits, temp_c) greatest_status_bits_temp_c
    from device_readings
    where reading_ts >= date '2025-09-01'
),
remaining_keys as (
    -- Set difference on keys
    select device_code
    from base
    except
    select device_code
    from device_readings
    where firmware != '2.0.1'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.device_code = k.device_code
),
valid as (
    -- Keep recent, valid rows
    select greatest_status_bits_temp_c, cast(reading_ts as date) reading_ts_day
    from reconciled
    where device_code = 'DV-04'
),
with_windows as (
    -- Running and moving aggregates
    select
        greatest_status_bits_temp_c,
        reading_ts_day,
        percent_rank() over (order by greatest_status_bits_temp_c desc) greatest_status_bits_temp_c_percent_rank,
        percent_rank() over (order by greatest_status_bits_temp_c desc nulls first) greatest_status_bits_temp_c_percent_rank_2,
        percent_rank() over (order by greatest_status_bits_temp_c desc) greatest_status_bits_temp_c_percent_rank_3
    from valid
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (order by greatest_status_bits_temp_c_percent_rank_2 nulls last) greatest_status_bits_temp_c_percent_rank_2_percent_rank,
        dense_rank() over (order by greatest_status_bits_temp_c_percent_rank_2) greatest_status_bits_temp_c_percent_rank_2_dense_rank,
        cume_dist() over (order by greatest_status_bits_temp_c_percent_rank desc) greatest_status_bits_temp_c_percent_rank_cume_dist,
        cume_dist() over (order by greatest_status_bits_temp_c_percent_rank_2) greatest_status_bits_temp_c_percent_rank_2_cume_dist
    from with_windows
),
stats as (
    -- Multi-level aggregation
    select
        case when greatest_status_bits_temp_c_percent_rank_2 >= 0.75 then 'PREMIUM' else 'STANDARD' end as greatest_status_bits_temp_c_percent_rank_2_tier,
        date_trunc('month', reading_ts_day) as activity_month,
        sum(greatest_status_bits_temp_c) as total_greatest_status_bits_temp_c,
        round(avg(greatest_status_bits_temp_c), 2) as avg_greatest_status_bits_temp_c,
        round(avg(greatest_status_bits_temp_c), 2) as avg_greatest_status_bits_temp_c_2,
        max(greatest_status_bits_temp_c_percent_rank_2_dense_rank) as max_greatest_status_bits_temp_c_percent_rank_2_dense_rank,
        count(*) as row_count,
        approx_percentile(greatest_status_bits_temp_c_percent_rank_cume_dist, 0.9) as greatest_status_bits_temp_c_percent_rank_cume_dist_p90
    from metrics
    group by 1, 2
)
select
    greatest_status_bits_temp_c_percent_rank_2_tier,
    activity_month,
    avg_greatest_status_bits_temp_c,
    avg_greatest_status_bits_temp_c_2,
    max_greatest_status_bits_temp_c_percent_rank_2_dense_rank,
    greatest_status_bits_temp_c_percent_rank_cume_dist_p90
from stats
