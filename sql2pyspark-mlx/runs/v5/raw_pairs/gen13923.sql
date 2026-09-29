with with_windows as (
    -- Ranking and gap features
    select
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        max(humidity) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_max_humidity,
        avg(humidity) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_avg_humidity,
        min(humidity) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as running_min_humidity
    from device_readings
    where temp_c >= 38
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by device_code order by humidity desc, id) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        running_max_humidity,
        running_avg_humidity,
        running_min_humidity,
        last_value(running_min_humidity, true) over (order by status_bits, id rows between unbounded preceding and current row) as last_known_running_min_humidity,
        ntile(10) over (order by running_min_humidity desc, id) as running_min_humidity_bucket,
        lead(running_avg_humidity, 1) over (partition by device_code order by running_max_humidity, id) as next_running_avg_humidity,
        lead(running_avg_humidity, 1, 0) over (partition by device_code order by dedup_rank, id) as next_running_avg_humidity_2,
        ntile(3) over (partition by device_code order by status_bits, id) as status_bits_bucket
    from latest
),
latest_device_readings as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by device_code order by next_running_avg_humidity desc, id) as row_num
        from scored
    ) t
    where row_num = 1
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        device_code,
        next_running_avg_humidity_2,
        running_avg_humidity,
        next_running_avg_humidity,
        running_max_humidity
    from latest_device_readings
) src
unpivot (
    val for metric in (next_running_avg_humidity_2, running_avg_humidity, next_running_avg_humidity, running_max_humidity)
) long_rows;
