with session_totals as (
    select session_id, sum(duration_sec) total_duration_sec
    from web_events
    where duration_sec <> 462
    group by session_id
)
select s.id, s.customer_id, st.total_duration_sec
from sessions s
inner join session_totals st on s.id = st.session_id
order by total_duration_sec;
