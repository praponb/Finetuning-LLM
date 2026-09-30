with with_prev as (
    select *, lag(event_time) over (partition by page order by event_time, id) as prev_event_time
    from web_events
),
flagged as (
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '7' day then 1 else 0 end as session_start_flag
    from with_prev
),
sessions_web_events as (
    select
        id,
        page,
        event_time,
        sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged
)
select
    date_trunc('week', event_time) as week_start,
    sort_array(collect_set(page)) as page_list,
    min(session_no) as min_session_no,
    grouping(date_trunc('week', event_time)) as grouping_id
from sessions_web_events
group by cube (date_trunc('week', event_time));
