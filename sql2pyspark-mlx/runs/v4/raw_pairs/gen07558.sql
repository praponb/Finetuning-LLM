with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        floor(humidity) as humidity_whole,
        lower(device_code) as device_code_lower
    from device_readings
    where temp_c <= 30
)
-- Window metrics over several different partitions
select
    *,
    rank() over (partition by device_code order by humidity_whole desc nulls first) as humidity_whole_rank,
    row_number() over (partition by device_code order by temp_c desc, id) as row_seq,
    avg(humidity_whole) over (partition by device_code order by cast(reading_ts as bigint) range between 7200 preceding and current row) as humidity_whole_avg_2h,
    dense_rank() over (partition by device_code order by humidity desc) as humidity_dense_rank,
    avg(humidity_whole) over (partition by device_code order by reading_ts, id rows between 2 preceding and 1 preceding) as humidity_whole_avg_2_prior
from filtered
