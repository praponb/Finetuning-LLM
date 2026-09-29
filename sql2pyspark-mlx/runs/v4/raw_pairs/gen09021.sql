with with_windows as (
    select
        *,
        avg(humidity) over (order by reading_ts, id rows between 2 preceding and 1 preceding) humidity_avg_2_prior,
        sum(temp_c) over (partition by device_code order by humidity, id rows between unbounded preceding and current row) running_temp_c,
        ntile(5) over (partition by device_code order by humidity desc, id) humidity_bucket,
        dense_rank() over (order by humidity) humidity_dense_rank,
        first_value(pressure) ignore nulls over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) first_known_pressure
    from device_readings
),
prev_events as (
    select *, lag(reading_ts) over (partition by device_code order by reading_ts, id) as prev_reading_ts
    from with_windows
),
flagged as (
    select *, case when prev_reading_ts is null or reading_ts > prev_reading_ts + interval 20 minutes then 1 else 0 end as is_new_burst
    from prev_events
),
islands as (
    select *, sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
latest as (
    select *
    from (
        select *, row_number() over (partition by device_code order by prev_reading_ts, id) as row_num
        from islands
    ) x
    where row_num = 1
),
kept_keys as (
    select device_code
    from latest
    except
    select device_code
    from device_readings
    where pressure between 1006 and 1018
),
reconciled as (
    select s.*
    from latest s
    join kept_keys k on s.device_code = k.device_code
)
select
    device_code,
    case when humidity_bucket >= 7 then 'HIGH' else 'LOW' end as humidity_bucket_tier,
    count(*) as row_count,
    sort_array(collect_set(device_code)) as device_code_list,
    approx_percentile(status_bits, array(0.25, 0.5, 0.75)) as status_bits_quartiles,
    round(sum(is_new_burst) / nullif(count(case when device_code = 'DV-04' then 1 end), 0), 2) as is_new_burst_per_dv_04,
    count(distinct id) as distinct_ids,
    count_if(device_code = 'DV-03') as dv_03_events
from reconciled
group by device_code, case when humidity_bucket >= 7 then 'HIGH' else 'LOW' end
order by row_count desc nulls first
