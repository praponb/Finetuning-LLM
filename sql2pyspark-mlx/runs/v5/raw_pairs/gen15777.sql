with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when reading_ts - lag(reading_ts) over (partition by device_code order by reading_ts, id) > interval 20 minutes then 1 else 0 end as is_new_burst
    from device_readings
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        sum(is_new_burst) over (partition by device_code order by reading_ts, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
scored as (
    -- Running and moving aggregates
    select
        id,
        temp_c,
        status_bits,
        burst_id,
        dense_rank() over w_running as burst_id_dense_rank,
        lag(humidity, 1) over (partition by device_code order by humidity, id) as prev_humidity
    from islands
    window w_running as (order by burst_id)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        temp_c,
        status_bits,
        prev_humidity,
        round(burst_id_dense_rank * 0.85, 2) as burst_id_dense_rank_scaled
    from scored
    where prev_humidity < 54
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        prev_humidity,
        burst_id_dense_rank_scaled,
        avg(prev_humidity) over (order by temp_c, id rows between 2 preceding and current row) as prev_humidity_avg_2,
        row_number() over w_hist as event_seq,
        ntile(4) over (order by status_bits desc, id) as status_bits_bucket
    from cleaned
    where burst_id_dense_rank_scaled between 46 and 51
    window w_hist as (order by burst_id_dense_rank_scaled, id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        prev_humidity,
        event_seq,
        status_bits_bucket,
        coalesce(burst_id_dense_rank_scaled, 0) as burst_id_dense_rank_scaled_filled,
        prev_humidity - event_seq as prev_humidity_minus_event_seq
    from with_windows
    where prev_humidity_avg_2 between 39 and 49
)
-- Multi-level aggregation
select
    case when prev_humidity_minus_event_seq >= 65 then 'HIGH' else 'LOW' end as prev_humidity_minus_event_seq_tier,
    case when status_bits_bucket >= 6 then 'PREMIUM' else 'STANDARD' end as status_bits_bucket_tier,
    approx_percentile(prev_humidity, 0.95) as prev_humidity_p95,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    count(distinct id) as distinct_ids_3,
    approx_percentile(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles,
    approx_percentile(status_bits_bucket, array(0.25, 0.5, 0.75)) as status_bits_bucket_quartiles,
    grouping(case when prev_humidity_minus_event_seq >= 65 then 'HIGH' else 'LOW' end) as grouping_id
from base
group by grouping sets ((case when prev_humidity_minus_event_seq >= 65 then 'HIGH' else 'LOW' end, case when status_bits_bucket >= 6 then 'PREMIUM' else 'STANDARD' end), (case when prev_humidity_minus_event_seq >= 65 then 'HIGH' else 'LOW' end), ())
order by distinct_ids nulls first;
