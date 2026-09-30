with scored as (
    -- Ranking and gap features
    select
        *,
        last_value(humidity) ignore nulls over (order by pressure, id rows between unbounded preceding and current row) last_known_humidity,
        ntile(3) over (partition by device_code order by status_bits, id) status_bits_bucket
    from device_readings
    where temp_c <= 31
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by device_code order by status_bits_bucket desc) as rank_in_group
    from scored
    qualify rank_in_group = 1
),
deduped_device_readings as (
    -- Deduplicate with ROW_NUMBER
    select *
    from deduped
    qualify row_number() over (partition by device_code order by status_bits, id) = 1
),
deduped_device_readings_2 as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by device_code order by status_bits desc, id) as latest_rank
    from deduped_device_readings
    qualify latest_rank = 1
)
-- Ranking and gap features
select
    id,
    device_code,
    temp_c,
    humidity,
    status_bits,
    firmware,
    last_known_humidity,
    status_bits_bucket,
    latest_rank,
    round(last_known_humidity / sum(last_known_humidity) over (partition by device_code), 4) last_known_humidity_share,
    round(temp_c / sum(temp_c) over (partition by device_code), 4) temp_c_share
from deduped_device_readings_2
