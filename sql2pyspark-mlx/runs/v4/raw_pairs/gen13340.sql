with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(reading_ts) over (partition by device_code order by reading_ts, id) is null
                 or reading_ts > lag(reading_ts) over (partition by device_code order by reading_ts, id) + interval 30 minutes then 1 else 0 end as is_new_burst
    from device_readings
),
sessions_device_readings as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
),
kept_keys as (
    -- Set intersection on keys
    select device_code
    from sessions_device_readings
    intersect all
    select device_code
    from device_readings
    where temp_c < 26
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_device_readings s
    join kept_keys k on s.device_code = k.device_code
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        is_new_burst,
        island_id,
        cume_dist() over w_hist as is_new_burst_cume_dist,
        stddev_samp(island_id) over w_running as device_code_stddev_samp_island_id,
        round(pressure / sum(pressure) over (partition by device_code), 4) as pressure_share,
        sum(is_new_burst) over w_ordered as is_new_burst_sum_1h,
        corr(pressure, humidity) over w_running as corr_pressure_humidity
    from reconciled
    window w_hist as (partition by device_code order by is_new_burst nulls last), w_running as (partition by device_code), w_ordered as (partition by device_code order by cast(reading_ts as bigint) range between 3600 preceding and current row)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        is_new_burst_sum_1h / nullif(status_bits, 0) as is_new_burst_sum_1h_per_status_bits,
        island_id + status_bits as island_id_plus_status_bits
    from metrics
    where reading_ts > date '2025-09-01'
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        min(island_id) over w_grp as device_code_min_island_id,
        count(humidity) over w_grp as device_code_count_humidity
    from valid
    window w_grp as (partition by device_code)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        max(pressure_share) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as pressure_share_max_30m,
        rank() over (partition by device_code order by island_id_plus_status_bits) as island_id_plus_status_bits_rank
    from enriched
),
metrics_device_readings as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by device_code order by pressure nulls first) as pressure_dense_rank,
        sum(pressure) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as pressure_sum_30m,
        cume_dist() over (partition by device_code order by corr_pressure_humidity desc nulls first) as corr_pressure_humidity_cume_dist,
        stddev_samp(device_code_stddev_samp_island_id) over (partition by device_code) as device_code_stddev_samp_device_code_stddev_samp_island_id,
        min(island_id) over (partition by device_code) as device_code_min_island_id_2
    from with_windows
)
select
    device_code,
    reading_ts,
    status_bits,
    firmware,
    island_id,
    is_new_burst_cume_dist,
    pressure_share,
    corr_pressure_humidity,
    is_new_burst_sum_1h_per_status_bits
from metrics_device_readings;
