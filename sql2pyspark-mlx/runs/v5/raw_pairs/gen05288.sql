with base as (
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        substring(event_type, 1, 4) as event_type_prefix
    from web_events
    where page is not null
      and page in ('/home', '/checkout', '/cart')
),
latest as (
    select *
    from (
        select *, rank() over (partition by session_id, event_type order by duration_sec desc nulls last) as dedup_rank
        from base
    ) x
    where dedup_rank = 1
),
with_prev as (
    select *, lag(event_time) over (partition by event_type order by event_time, id) as prev_event_time
    from latest
),
gaps as (
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '30' day then 1 else 0 end as is_new_session
    from with_prev
),
islands as (
    select *, sum(is_new_session) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
with_windows as (
    select
        *,
        row_number() over (partition by event_type order by is_new_session desc, id) as seq_no,
        max(duration_sec) over (partition by page) as page_max_duration_sec
    from islands
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.duration_sec,
    s.event_type_prefix,
    s.is_new_session,
    s.island_id,
    (select min(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as min_web_event_duration_sec
from with_windows s
order by min_web_event_duration_sec desc nulls first
