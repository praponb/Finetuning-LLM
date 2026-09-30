with ranked as (
    select *
    from web_events
    qualify row_number() over (partition by session_id order by duration_sec, id) = 1
),
ranked_web_events as (
    select *, row_number() over (partition by session_id order by event_time desc nulls last, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
flagged as (
    select *, case when event_time - lag(event_time) over (partition by event_type order by event_time, id) > interval '90' day then 1 else 0 end as is_new_burst
    from ranked_web_events
),
islands as (
    select
        id,
        event_type,
        page,
        event_time,
        latest_rank,
        sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
valid as (
    select
        id,
        event_type,
        page,
        latest_rank,
        session_no,
        upper(event_type) as event_type_upper,
        coalesce(page, 'N/A') as page_filled
    from islands
    where event_time > date '2023-06-18'
),
enriched as (
    select
        id,
        event_type,
        page,
        event_type_upper,
        page_filled,
        lead(session_no) over (partition by event_type order by session_no, id) as next_session_no,
        row_number() over (partition by event_type order by session_no, id) as event_seq,
        lead(session_no, 1) over (partition by page order by session_no, id) as next_session_no_2
    from valid
    where page != '/checkout'
)
select
    id,
    event_type,
    event_type_upper,
    page_filled,
    next_session_no
from enriched
order by event_type desc
