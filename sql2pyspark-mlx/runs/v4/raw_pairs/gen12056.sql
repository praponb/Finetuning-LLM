with metrics as (
    -- Running and moving aggregates
    select
        *,
        corr(humidity, status_bits) over (partition by device_code) as corr_humidity_status_bits,
        last_value(temp_c, true) over (partition by device_code order by temp_c, id rows between unbounded preceding and current row) as last_known_temp_c,
        last_value(humidity) ignore nulls over (partition by device_code order by humidity, id rows between unbounded preceding and current row) as last_known_humidity
    from device_readings
)
-- Aggregate with conditional metrics
select
    date_trunc('day', reading_ts) as activity_day,
    case when pressure >= 1019 then 'LARGE' else 'SMALL' end as pressure_tier,
    bit_or(status_bits) as status_bits_bit_or,
    count(distinct device_code) as distinct_device_codes,
    count(*) as cnt,
    percent_rank() over (partition by date_trunc('day', reading_ts) order by bit_or(status_bits) desc) as status_bits_bit_or_percent_rank
from metrics
group by date_trunc('day', reading_ts), case when pressure >= 1019 then 'LARGE' else 'SMALL' end
