with cleaned as (
    -- Keep recent, valid rows
    select *, humidity + status_bits as humidity_plus_status_bits
    from device_readings
    where humidity > 68
),
cleaned_device_readings as (
    -- Filter the raw rows and derive helper columns
    select *, cast(pressure as int) as pressure_int
    from cleaned
    where reading_ts > timestamp '2025-09-01 04:00:00'
),
cleaned_device_readings_2 as (
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
        pressure_int,
        pressure_int - humidity as pressure_int_minus_humidity
    from cleaned_device_readings
    where status_bits between 5 and 10
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(status_bits) over (partition by device_code order by humidity, id) as next_status_bits,
        lead(pressure_int) over (partition by device_code order by humidity, id) as next_pressure_int,
        row_number() over (partition by device_code order by humidity, id) as event_seq
    from cleaned_device_readings_2
)
-- Grouped metrics
select
    date_trunc('day', reading_ts) as activity_day,
    case when next_pressure_int >= 82 then 'LARGE' else 'SMALL' end as next_pressure_int_tier,
    device_code,
    size(collect_set(device_code)) as num_device_codes,
    count_if(device_code = 'DV-04') as dv_04_events,
    min(reading_ts) as first_reading_ts,
    sum(case when device_code = 'DV-04' then status_bits else 0 end) as dv_04_status_bits
from enriched
group by 1, 2, 3
