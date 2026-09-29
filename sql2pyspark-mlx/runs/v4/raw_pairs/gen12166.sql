with with_windows as (
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
        ntile(5) over (order by status_bits, id) as status_bits_bucket,
        sum(status_bits) over (partition by device_code order by status_bits, id rows between unbounded preceding and current row) as cumulative_status_bits,
        sum(pressure) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_pressure,
        sum(humidity) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as running_humidity
    from device_readings
),
with_windows_device_readings as (
    -- Ranking and gap features
    select
        *,
        corr(humidity, pressure) over (partition by device_code) as corr_humidity_pressure,
        sum(status_bits) over (partition by device_code) as device_code_sum_status_bits,
        first_value(cumulative_status_bits, true) over (order by temp_c, id rows between unbounded preceding and current row) as first_known_cumulative_status_bits
    from with_windows
),
scored as (
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
        status_bits_bucket,
        cumulative_status_bits,
        running_pressure,
        running_humidity,
        corr_humidity_pressure,
        device_code_sum_status_bits,
        first_known_cumulative_status_bits,
        dense_rank() over (partition by device_code order by corr_humidity_pressure desc) as corr_humidity_pressure_dense_rank,
        sum(first_known_cumulative_status_bits) over (partition by device_code order by first_known_cumulative_status_bits, id rows between unbounded preceding and current row) as cumulative_first_known_cumulative_status_bits,
        dense_rank() over (partition by device_code order by status_bits) as status_bits_dense_rank
    from with_windows_device_readings
),
valid as (
    -- Keep recent, valid rows
    select *, floor(first_known_cumulative_status_bits) as first_known_cumulative_status_bits_whole
    from scored
    where status_bits_bucket < 3
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by device_code order by running_humidity desc) as rn
    from valid
    qualify rn = 1
),
latest_device_readings as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by rn, id) as latest_rank
    from latest
    qualify latest_rank = 1
)
-- Multi-level aggregation
select
    date_trunc('hour', reading_ts) as hour_window,
    max(reading_ts) as last_seen,
    sum(cumulative_first_known_cumulative_status_bits) as total_cumulative_first_known_cumulative_status_bits,
    approx_percentile(device_code_sum_status_bits, array(0.25, 0.5, 0.75)) as device_code_sum_status_bits_quartiles
from latest_device_readings
group by 1
order by hour_window nulls last;
