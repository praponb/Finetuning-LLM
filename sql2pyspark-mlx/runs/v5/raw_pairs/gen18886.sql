with valid as (
    -- Keep recent, valid rows
    select
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        greatest(pressure, temp_c) as greatest_pressure_temp_c
    from device_readings
    where reading_ts > date '2025-09-01'
      and status_bits between 6 and 10
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        reading_ts,
        pressure,
        greatest_pressure_temp_c,
        round(greatest_pressure_temp_c * 100, 2) as greatest_pressure_temp_c_scaled
    from valid
    where reading_ts > timestamp '2025-09-01 08:00:00'
),
matched_keys as (
    -- Set difference on keys
    select id
    from filtered
    except
    select id
    from device_readings
    where device_code = 'DV-04'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.id = k.id
),
enriched as (
    -- Ranking and gap features
    select
        *,
        max(pressure) over (order by cast(reading_ts as bigint) range between 7200 preceding and current row) as pressure_max_2h,
        max(greatest_pressure_temp_c_scaled) over (order by cast(reading_ts as bigint) range between 1800 preceding and current row) as greatest_pressure_temp_c_scaled_max_30m
    from verified
),
metrics as (
    -- Ranking and gap features
    select
        *,
        count(pressure_max_2h) over w_recent as pressure_max_2h_count_15m,
        count(pressure) over w_hist as pressure_count_2h,
        rank() over (order by pressure_max_2h desc) as pressure_max_2h_rank
    from enriched
    window w_recent as (order by cast(reading_ts as bigint) range between 900 preceding and current row), w_hist as (order by cast(reading_ts as bigint) range between 7200 preceding and current row)
),
valid_device_readings as (
    -- Keep recent, valid rows
    select
        id,
        reading_ts,
        pressure,
        greatest_pressure_temp_c,
        greatest_pressure_temp_c_scaled,
        pressure_max_2h,
        pressure_max_2h_count_15m,
        pressure_max_2h_rank,
        floor(pressure_max_2h_rank) as pressure_max_2h_rank_whole
    from metrics
    where greatest_pressure_temp_c_scaled_max_30m = 70
)
select
    id,
    reading_ts,
    greatest_pressure_temp_c,
    greatest_pressure_temp_c_scaled,
    pressure_max_2h,
    pressure_max_2h_count_15m,
    pressure_max_2h_rank,
    pressure_max_2h_rank_whole
from valid_device_readings
order by greatest_pressure_temp_c desc;
