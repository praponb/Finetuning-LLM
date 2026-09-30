with session_totals as (
    select session_id, min(event_time) min_event_time, sum(case when event_type = 'page_view' then duration_sec else 0 end) page_view_duration_sec
    from web_events
    group by session_id
)
select
    sessions.id,
    sessions.customer_id,
    sessions.started_at,
    session_totals.min_event_time,
    session_totals.page_view_duration_sec
from sessions
inner join session_totals on sessions.id = session_totals.session_id;
