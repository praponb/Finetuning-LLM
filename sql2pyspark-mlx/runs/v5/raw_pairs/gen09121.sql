with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(pressure, true) over (order by pressure asc, id asc rows between unbounded preceding and current row) as first_known_pressure,
        ntile(4) over w_running as status_bits_bucket,
        lag(temp_c, 1) over (partition by device_code order by pressure asc, id asc) as prev_temp_c,
        lead(pressure, 1, 0) over w_running as next_pressure,
        first_value(pressure, true) over (partition by device_code order by status_bits asc, id asc rows between unbounded preceding and current row) as first_known_pressure_2
    from device_readings
    window w_running as (partition by device_code order by status_bits asc, id asc)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        first_known_pressure,
        status_bits_bucket,
        next_pressure,
        first_known_pressure_2,
        round(status_bits_bucket / prev_temp_c, 2) as status_bits_bucket_per_prev_temp_c
    from with_windows
    where device_code = 'DV-04'
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        device_code,
        reading_ts,
        firmware,
        status_bits_bucket,
        status_bits_bucket_per_prev_temp_c
    from filtered
    qualify row_number() over (partition by device_code order by humidity desc, id asc) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        max(status_bits_bucket) over (partition by device_code order by status_bits_bucket asc, id asc rows between 3 preceding and current row) as last4_max_status_bits_bucket,
        avg(status_bits_bucket) over (partition by device_code order by status_bits_bucket asc, id asc rows between 3 preceding and current row) as last4_avg_status_bits_bucket,
        count(status_bits_bucket) over (partition by device_code order by status_bits_bucket asc, id asc rows between 3 preceding and current row) as last4_count_status_bits_bucket,
        min(status_bits_bucket) over (partition by device_code order by status_bits_bucket asc, id asc rows between 3 preceding and current row) as last4_min_status_bits_bucket,
        dense_rank() over (partition by device_code order by status_bits_bucket_per_prev_temp_c desc) as status_bits_bucket_per_prev_temp_c_dense_rank
    from deduped
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts asc, id asc) > interval '1' hour then 1 else 0 end as session_start_flag
    from metrics
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        firmware,
        status_bits_bucket,
        last4_max_status_bits_bucket,
        last4_avg_status_bits_bucket,
        last4_min_status_bits_bucket,
        sum(session_start_flag) over (partition by device_code order by reading_ts asc, id asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
filtered_device_readings as (
    -- Filter the raw rows and derive helper columns
    select *, greatest(last4_min_status_bits_bucket, burst_id) as greatest_last4_min_status_bits_bucket_burst_id
    from islands
    where reading_ts > timestamp '2025-09-01 09:00:00'
      and hour(reading_ts) between 9 and 12
)
-- Multi-level aggregation
select
    burst_id,
    case when last4_min_status_bits_bucket >= 8.1 then 'HIGH' else 'LOW' end as last4_min_status_bits_bucket_tier,
    device_code,
    max(last4_avg_status_bits_bucket) filter (where device_code = 'DV-01') as dv_01_max_last4_avg_status_bits_bucket,
    count(case when device_code = 'DV-03' then 1 end) as dv_03_count,
    percentile_approx(last4_min_status_bits_bucket, array(0.25, 0.5, 0.75)) as last4_min_status_bits_bucket_quartiles,
    round(sum(last4_min_status_bits_bucket) / nullif(count(case when device_code = 'DV-04' then 1 end), 0), 2) as last4_min_status_bits_bucket_per_dv_04,
    max(reading_ts) as last_reading_ts,
    count_if(device_code = 'DV-01') as dv_01_events
from filtered_device_readings
group by grouping sets ((burst_id, case when last4_min_status_bits_bucket >= 8.1 then 'HIGH' else 'LOW' end, device_code), (burst_id, case when last4_min_status_bits_bucket >= 8.1 then 'HIGH' else 'LOW' end), (burst_id), ())
order by dv_03_count asc;
