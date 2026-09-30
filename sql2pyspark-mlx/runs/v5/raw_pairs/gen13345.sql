with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval '60' day then 1 else 0 end as is_new_burst
    from web_events
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        sum(is_new_burst) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from gaps
)
-- Multi-level aggregation
select
    session_no,
    sum(case when page = '/home' then duration_sec else 0 end) as home_duration_sec,
    sort_array(collect_set(page)) as page_list,
    sort_array(collect_set(page)) as page_list_2
from sessions_web_events
group by 1
