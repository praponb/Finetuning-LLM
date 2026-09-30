with scored as (
    -- Running and moving aggregates
    select
        *,
        max(status_bits) over (partition by device_code order by cast(reading_ts as bigint) range between 1800 preceding and current row) as status_bits_max_30m,
        avg(status_bits) over (order by humidity, id rows between 5 preceding and 1 preceding) as status_bits_avg_5_prior,
        rank() over (partition by device_code order by pressure desc) as pressure_rank
    from device_readings
)
-- Aggregate with conditional metrics
select
    case when pressure >= 1024 then 'PREMIUM' else 'STANDARD' end as pressure_tier,
    case when status_bits_avg_5_prior >= 12 then 'LARGE' else 'SMALL' end as status_bits_avg_5_prior_tier,
    (unix_timestamp(max(reading_ts)) - unix_timestamp(min(reading_ts))) / 60 as duration_min,
    min(reading_ts) as first_reading_ts,
    count(distinct id) as distinct_ids,
    size(collect_set(device_code)) as num_device_codes
from scored
group by case when pressure >= 1024 then 'PREMIUM' else 'STANDARD' end, case when status_bits_avg_5_prior >= 12 then 'LARGE' else 'SMALL' end;
