with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits * temp_c as status_bits_times_temp_c
    from device_readings
    where reading_ts > to_date('2025-09-01')
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_times_temp_c,
        row_number() over (partition by device_code order by humidity desc, id) as row_seq,
        lag(pressure, 1) over (partition by device_code order by status_bits_times_temp_c, id) as prev_pressure,
        ntile(4) over (partition by device_code order by pressure, id) as pressure_bucket,
        count(humidity) over (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row) as humidity_count_2h
    from valid
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by device_code order by pressure_bucket nulls first, id) as rn
    from metrics
    qualify rn = 1
)
-- Native SQL PIVOT
select *
from (
    select
        firmware,
        device_code,
        pressure
    from latest
) src
pivot (
    avg(pressure) as value_sum,
    count(1) as n
    for device_code in ('DV-04' as dv_04, 'DV-03' as dv_03)
);
