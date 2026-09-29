with base as (
    select
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        round(humidity, 2) as humidity_rounded
    from device_readings
    where temp_c >= 28
),
remaining_keys as (
    select id, firmware
    from base
    intersect all
    select id, firmware
    from device_readings
    where status_bits = 10
),
kept_rows as (
    select s.*
    from base s
    inner join remaining_keys k on s.id = k.id and s.firmware = k.firmware
),
remaining_keys_2 as (
    select firmware, device_code
    from kept_rows
    intersect all
    select firmware, device_code
    from device_readings
    where reading_ts < '2023-07-01'
),
kept_rows_device_readings as (
    select s.*
    from kept_rows s
    inner join remaining_keys_2 k on s.firmware = k.firmware and s.device_code = k.device_code
),
enriched as (
    select
        *,
        dense_rank() over w_grp as humidity_rounded_dense_rank,
        corr(humidity, pressure) over w_running as corr_humidity_pressure,
        corr(pressure, status_bits) over w_running as corr_pressure_status_bits
    from kept_rows_device_readings
    window w_grp as (partition by device_code order by humidity_rounded), w_running as (partition by device_code)
),
with_windows as (
    select
        *,
        stddev_samp(humidity_rounded_dense_rank) over w_grp as device_code_stddev_samp_humidity_rounded_dense_rank,
        avg(corr_humidity_pressure) over w_grp as device_code_avg_corr_humidity_pressure,
        min(status_bits) over w_grp as device_code_min_status_bits
    from enriched
    window w_grp as (partition by device_code)
),
filtered as (
    select
        id,
        device_code,
        humidity,
        status_bits,
        firmware,
        humidity_rounded,
        corr_humidity_pressure,
        corr_pressure_status_bits,
        device_code_stddev_samp_humidity_rounded_dense_rank,
        device_code_avg_corr_humidity_pressure,
        pressure * humidity_rounded_dense_rank as pressure_times_humidity_rounded_dense_rank
    from with_windows
    where corr_humidity_pressure > -0.1
),
metrics as (
    select
        id,
        device_code,
        humidity,
        status_bits,
        firmware,
        humidity_rounded,
        corr_humidity_pressure,
        corr_pressure_status_bits,
        device_code_stddev_samp_humidity_rounded_dense_rank,
        device_code_avg_corr_humidity_pressure,
        pressure_times_humidity_rounded_dense_rank,
        corr(status_bits, humidity) over w_ordered as corr_status_bits_humidity,
        avg(corr_pressure_status_bits) over w_ordered as device_code_avg_corr_pressure_status_bits,
        sum(status_bits) over w_ordered as device_code_sum_status_bits,
        rank() over w_grp as corr_pressure_status_bits_rank
    from filtered
    window w_ordered as (partition by device_code), w_grp as (partition by device_code order by corr_pressure_status_bits nulls first)
),
grouped as (
    select
        device_code,
        count(distinct id) as distinct_ids,
        sort_array(collect_set(device_code)) as device_code_list
    from metrics
    group by 1
)
select
    device_code,
    distinct_ids,
    device_code_list
from grouped
order by device_code desc nulls first
