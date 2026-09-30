with cleaned as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        coalesce(humidity, 0) as humidity_filled
    from device_readings
    where reading_ts > to_timestamp('2025-09-01 08:00:00')
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by device_code order by temp_c desc, id asc) as dedup_rank
        from cleaned
    ) t
    where dedup_rank = 1
),
with_windows as (
    select
        *,
        max(humidity) over (partition by device_code order by dedup_rank asc, id asc rows between unbounded preceding and current row) as running_max_humidity,
        min(humidity) over (partition by device_code order by dedup_rank asc, id asc rows between unbounded preceding and current row) as running_min_humidity,
        sum(humidity) over (partition by device_code order by dedup_rank asc, id asc rows between unbounded preceding and current row) as running_sum_humidity,
        round(temp_c / sum(temp_c) over (partition by device_code), 4) as temp_c_share
    from ranked
    where temp_c >= 34
),
enriched as (
    select
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        dedup_rank,
        running_sum_humidity,
        temp_c_share,
        sum(humidity_filled) over (order by running_max_humidity asc, id asc rows between unbounded preceding and current row) as running_humidity_filled,
        row_number() over (partition by device_code order by humidity asc, id asc) as row_seq
    from with_windows
)
select
    id,
    device_code,
    humidity,
    status_bits,
    running_sum_humidity,
    row_seq
from enriched
order by status_bits asc nulls first, device_code asc;
