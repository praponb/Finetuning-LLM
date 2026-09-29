with base as (
    -- Keep recent, valid rows
    select
        *,
        date_format(event_time, 'MMM yyyy') as event_time_label,
        coalesce(duration_sec, 0) as duration_sec_filled
    from web_events
    where event_time >= to_date('2023-06-14')
      and page is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by page order by event_time asc, id asc) is null
                 or event_time > lag(event_time) over (partition by page order by event_time asc, id asc) + interval '30' day then 1 else 0 end as is_new_session
    from base
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by page order by event_time asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by duration_sec_filled asc nulls last) as duration_sec_filled_dense_rank,
        stddev_samp(island_id) over (partition by page) as page_stddev_samp_island_id,
        avg(duration_sec) over w_ordered as duration_sec_avg_5
    from sessionized
    window w_ordered as (partition by event_type order by duration_sec asc, id asc rows between 5 preceding and current row)
),
metrics_web_events as (
    -- Window metrics over several different partitions
    select
        id,
        event_time,
        event_time_label,
        is_new_session,
        duration_sec_filled_dense_rank,
        duration_sec_avg_5,
        sum(duration_sec_filled_dense_rank) over (partition by event_type order by island_id asc, id asc rows between 6 preceding and current row) as last7_sum_duration_sec_filled_dense_rank,
        count(duration_sec_filled_dense_rank) over (partition by event_type order by island_id asc, id asc rows between 6 preceding and current row) as last7_count_duration_sec_filled_dense_rank,
        min(duration_sec_filled_dense_rank) over (partition by event_type order by island_id asc, id asc rows between 6 preceding and current row) as last7_min_duration_sec_filled_dense_rank,
        max(duration_sec_filled_dense_rank) over (partition by event_type order by island_id asc, id asc rows between 6 preceding and current row) as last7_max_duration_sec_filled_dense_rank
    from metrics
),
scored as (
    -- Running and moving aggregates
    select
        id,
        event_time_label,
        is_new_session,
        last7_sum_duration_sec_filled_dense_rank,
        last7_count_duration_sec_filled_dense_rank,
        row_number() over (order by last7_max_duration_sec_filled_dense_rank asc, id asc) as event_seq,
        row_number() over (order by last7_count_duration_sec_filled_dense_rank desc, id asc) as event_seq_2,
        dense_rank() over (order by duration_sec_avg_5 asc nulls first) as duration_sec_avg_5_dense_rank
    from metrics_web_events
)
-- Window metrics over several different partitions
select
    id,
    event_time_label,
    last7_count_duration_sec_filled_dense_rank,
    event_seq,
    duration_sec_avg_5_dense_rank,
    dense_rank() over (order by event_seq_2 desc nulls first) as event_seq_2_dense_rank,
    last_value(event_seq, true) over (order by event_seq_2 asc, id asc rows between unbounded preceding and current row) as last_known_event_seq
from scored
where last7_sum_duration_sec_filled_dense_rank >= 27;
