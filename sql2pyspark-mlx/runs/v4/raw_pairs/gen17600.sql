with session_totals as (
    select session_id, max(event_time) as latest_event_time, round(avg(duration_sec), 2) as avg_duration_sec
    from web_events
    where event_type = 'search'
    group by session_id
)
select
    s.id,
    s.started_at,
    st.latest_event_time,
    st.avg_duration_sec
from sessions s
inner join session_totals st on s.id = st.session_id
order by s.started_at desc, st.latest_event_time, id
limit 5
