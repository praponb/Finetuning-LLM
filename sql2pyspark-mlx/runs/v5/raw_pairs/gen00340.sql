with session_totals as (
    select session_id, round(avg(duration_sec), 2) as avg_duration_sec
    from web_events
    group by session_id
)
select s.customer_id, s.started_at, st.avg_duration_sec
from sessions s
left outer join session_totals st on s.id = st.session_id
where st.avg_duration_sec > 34
order by 2;
