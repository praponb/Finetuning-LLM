with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        coalesce(humidity, 0) as humidity_filled
    from device_readings
    where reading_ts > timestamp '2025-09-01 14:00:00'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by device_code order by temp_c desc nulls first) as temp_c_percent_rank,
        max(humidity_filled) over (partition by device_code order by cast(reading_ts as bigint) asc range between 3600 preceding and current row) as humidity_filled_max_1h,
        corr(pressure, humidity) over (partition by device_code) as corr_pressure_humidity,
        dense_rank() over (partition by device_code order by pressure asc) as pressure_dense_rank
    from base
),
enriched_device_readings as (
    -- Window metrics over several different partitions
    select
        *,
        avg(temp_c) over (partition by device_code order by humidity asc, id asc rows between 5 preceding and 1 preceding) as temp_c_avg_5_prior,
        first_value(humidity_filled_max_1h) ignore nulls over (partition by device_code order by temp_c_percent_rank asc, id asc rows between unbounded preceding and current row) as first_known_humidity_filled_max_1h,
        count(temp_c_percent_rank) over (partition by device_code order by cast(reading_ts as bigint) asc range between 7200 preceding and current row) as temp_c_percent_rank_count_2h,
        avg(humidity_filled_max_1h) over (partition by device_code order by cast(reading_ts as bigint) asc range between 900 preceding and current row) as humidity_filled_max_1h_avg_15m,
        avg(humidity) over (partition by device_code order by humidity_filled asc, id asc rows between 2 preceding and 1 preceding) as humidity_avg_2_prior
    from enriched
),
enriched_device_readings_2 as (
    -- Running and moving aggregates
    select
        *,
        sum(temp_c_avg_5_prior) over w_hist as running_temp_c_avg_5_prior,
        first_value(temp_c_percent_rank_count_2h, true) over (partition by device_code order by humidity_filled_max_1h asc, id asc rows between unbounded preceding and current row) as first_known_temp_c_percent_rank_count_2h
    from enriched_device_readings
    window w_hist as (partition by device_code order by pressure asc, id asc rows between unbounded preceding and current row)
)
-- Multi-level aggregation
select
    case when pressure >= 1031 then 'HIGH' else 'LOW' end as pressure_tier,
    date_trunc('hour', reading_ts) as hour_window,
    corr(pressure, temp_c) as corr_pressure_temp_c,
    count(*) as num_rows,
    approx_percentile(humidity_avg_2_prior, array(0.25, 0.5, 0.75)) as humidity_avg_2_prior_quartiles,
    grouping_id() as grouping_level
from enriched_device_readings_2
group by cube (case when pressure >= 1031 then 'HIGH' else 'LOW' end, date_trunc('hour', reading_ts))
having count(*) < 1
order by num_rows desc nulls last
