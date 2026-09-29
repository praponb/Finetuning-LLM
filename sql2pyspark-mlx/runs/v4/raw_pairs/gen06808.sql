with latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by device_code order by status_bits, id) as row_num
    from device_readings
    qualify row_num = 1
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        row_num,
        concat_ws('-', firmware, device_code) as firmware_device_code_key
    from latest
    where reading_ts >= timestamp '2025-09-01 09:00:00'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        humidity,
        status_bits,
        firmware,
        row_num,
        floor(humidity) as humidity_whole
    from base
    where row_num != 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        humidity,
        firmware,
        humidity_whole,
        length(firmware) as firmware_length
    from valid
    where status_bits >= 4
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(humidity) over (order by humidity, id rows between unbounded preceding and current row) as cumulative_humidity,
        sum(firmware_length) over (order by firmware_length, id rows between unbounded preceding and current row) as cumulative_firmware_length,
        row_number() over (order by humidity_whole, id) as row_seq,
        avg(firmware_length) over (order by humidity_whole, id rows between 2 preceding and 1 preceding) as firmware_length_avg_2_prior
    from filtered
)
-- Running and moving aggregates
select
    *,
    sum(cumulative_humidity) over (order by humidity, id rows between unbounded preceding and current row) as cumulative_cumulative_humidity,
    cume_dist() over (order by cumulative_firmware_length) as cumulative_firmware_length_cume_dist
from metrics;
