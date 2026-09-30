with cleaned as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        cast(humidity as int) as humidity_int
    from device_readings
    where hour(reading_ts) between 11 and 14
),
enriched as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        ntile(3) over (partition by device_code order by temp_c desc, id) as temp_c_bucket,
        round(humidity / sum(humidity) over (partition by device_code), 4) as humidity_share
    from cleaned
),
enriched_device_readings as (
    select
        *,
        avg(pressure) over (partition by device_code order by cast(reading_ts as bigint) range between 900 preceding and current row) as pressure_avg_15m,
        dense_rank() over (partition by device_code order by humidity_share desc nulls last) as humidity_share_dense_rank
    from enriched
    where device_code = 'DV-04'
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by device_code order by humidity_share, id) as latest_rank
        from enriched_device_readings
    ) x
    where latest_rank = 1
),
with_prev as (
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from deduped
),
gaps as (
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval '20' minute then 1 else 0 end as is_new_session
    from with_prev
),
sessions_device_readings as (
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        pressure_avg_15m,
        latest_rank,
        sum(is_new_session) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as island_id
    from gaps
)
select
    case when pressure >= 1034 then 'LARGE' else 'SMALL' end as pressure_tier,
    date_trunc('hour', reading_ts) as hour_window,
    min(reading_ts) as first_reading_ts,
    sort_array(collect_set(device_code)) as device_code_list,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as duration_min
from sessions_device_readings
group by cube (case when pressure >= 1034 then 'LARGE' else 'SMALL' end, date_trunc('hour', reading_ts));
