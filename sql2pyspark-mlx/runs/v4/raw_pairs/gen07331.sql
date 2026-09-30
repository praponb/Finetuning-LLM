with cleaned as (
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        date_trunc('WEEK', event_time) as event_time_period,
        concat_ws('/', event_type, page) as event_type_page_key
    from web_events
    where event_time >= date '2023-08-21'
),
scored as (
    select
        *,
        count(duration_sec) over (partition by session_id order by cast(event_time_period as bigint) range between 604800 preceding and current row) as duration_sec_count_7d,
        max(duration_sec) over (partition by session_id order by cast(event_time as bigint) range between 2592000 preceding and current row) as duration_sec_max_30d,
        sum(duration_sec) over (partition by event_type order by event_time_period, id rows between unbounded preceding and current row) as cumulative_duration_sec,
        last_value(duration_sec) ignore nulls over (partition by session_id order by duration_sec, id rows between unbounded preceding and current row) as last_known_duration_sec
    from cleaned
),
latest as (
    select *
    from (
        select *, row_number() over (partition by session_id order by cumulative_duration_sec desc nulls first, id) as row_num
        from scored
    ) x
    where row_num = 1
)
select
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    event_time_period,
    event_type_page_key,
    duration_sec_count_7d,
    duration_sec_max_30d,
    cumulative_duration_sec,
    last_known_duration_sec,
    row_num,
    avg(cumulative_duration_sec) over w_ordered as cumulative_duration_sec_avg_2_prior,
    ntile(10) over w_grp as duration_sec_count_7d_bucket,
    row_number() over (partition by event_type order by duration_sec, id) as row_seq,
    sum(cumulative_duration_sec) over (partition by event_type, session_id order by cast(event_time as bigint) range between 604800 preceding and current row) as cumulative_duration_sec_sum_7d,
    rank() over w_grp_2 as duration_sec_rank
from latest
window w_ordered as (order by duration_sec_count_7d, id rows between 2 preceding and 1 preceding), w_grp as (partition by session_id, event_type order by duration_sec_count_7d, id), w_grp_2 as (partition by event_type, session_id order by duration_sec desc);
