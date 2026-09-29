with flagged as (
    select *, case when event_time - lag(event_time) over (partition by event_type order by event_time, id) > interval 60 days then 1 else 0 end as session_start_flag
    from web_events
),
islands as (
    select *, sum(session_start_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from flagged
),
filtered as (
    select
        id,
        duration_sec,
        date_format(event_time, 'yyyy-MM') as event_time_label
    from islands
    where event_time > date '2024-03-20'
),
filtered_web_events as (
    select
        *,
        case when duration_sec >= 376 then 'High' when duration_sec >= 125 then 'Medium' else 'Low' end as duration_sec_band,
        round(duration_sec * 100, 2) as duration_sec_adjusted
    from filtered
    where duration_sec between 194 and 222
),
metrics as (
    select
        *,
        first_value(duration_sec, true) over (order by duration_sec_adjusted, id rows between unbounded preceding and current row) as first_known_duration_sec,
        row_number() over (order by duration_sec_adjusted, id) as seq_no,
        ntile(4) over (order by duration_sec_adjusted, id) as duration_sec_adjusted_bucket
    from filtered_web_events
),
scored as (
    select
        *,
        sum(first_known_duration_sec) over (order by first_known_duration_sec, id rows between unbounded preceding and current row) as cumulative_first_known_duration_sec,
        lag(seq_no, 1) over (order by duration_sec_adjusted, id) as prev_seq_no,
        sum(first_known_duration_sec) over (order by duration_sec_adjusted_bucket, id rows between unbounded preceding and current row) as running_first_known_duration_sec,
        rank() over (order by duration_sec_adjusted_bucket desc nulls last) as duration_sec_adjusted_bucket_rank,
        sum(first_known_duration_sec) over (order by first_known_duration_sec, id rows between unbounded preceding and current row) as running_first_known_duration_sec_2
    from metrics
),
scored_web_events as (
    select
        id,
        duration_sec,
        event_time_label,
        duration_sec_band,
        duration_sec_adjusted,
        first_known_duration_sec,
        seq_no,
        duration_sec_adjusted_bucket,
        cumulative_first_known_duration_sec,
        prev_seq_no,
        running_first_known_duration_sec,
        duration_sec_adjusted_bucket_rank,
        running_first_known_duration_sec_2,
        ntile(4) over (order by seq_no, id) as seq_no_bucket,
        row_number() over w_recent as row_seq,
        dense_rank() over (order by duration_sec_adjusted_bucket_rank desc nulls last) as duration_sec_adjusted_bucket_rank_dense_rank
    from scored
    window w_recent as (order by running_first_known_duration_sec desc, id)
),
with_windows as (
    select
        id,
        duration_sec,
        event_time_label,
        duration_sec_band,
        duration_sec_adjusted,
        first_known_duration_sec,
        seq_no,
        duration_sec_adjusted_bucket,
        cumulative_first_known_duration_sec,
        prev_seq_no,
        running_first_known_duration_sec,
        duration_sec_adjusted_bucket_rank,
        running_first_known_duration_sec_2,
        seq_no_bucket,
        row_seq,
        duration_sec_adjusted_bucket_rank_dense_rank,
        sum(running_first_known_duration_sec_2) over (order by seq_no, id rows between unbounded preceding and current row) as cumulative_running_first_known_duration_sec_2,
        last_value(running_first_known_duration_sec_2, true) over (order by seq_no, id rows between unbounded preceding and current row) as last_known_running_first_known_duration_sec_2,
        avg(seq_no_bucket) over (order by running_first_known_duration_sec_2, id rows between 2 preceding and current row) as seq_no_bucket_avg_2
    from scored_web_events
),
grouped as (
    select
        case when prev_seq_no >= 7 then 'LARGE' else 'SMALL' end as prev_seq_no_tier,
        case when running_first_known_duration_sec_2 >= 2273 then 'LARGE' else 'SMALL' end as running_first_known_duration_sec_2_tier,
        bit_and(duration_sec) as duration_sec_bit_and,
        min(running_first_known_duration_sec_2) as min_running_first_known_duration_sec_2,
        count(distinct id) as distinct_ids,
        percentile_approx(seq_no_bucket, 0.9) as seq_no_bucket_p90
    from with_windows
    group by 1, 2
)
select
    prev_seq_no_tier,
    running_first_known_duration_sec_2_tier,
    duration_sec_bit_and,
    distinct_ids
from grouped
order by distinct_ids desc, running_first_known_duration_sec_2_tier
