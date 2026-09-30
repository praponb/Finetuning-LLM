with session_stats as (
    select session_id, count(duration_sec) as duration_sec_count
    from web_events
    group by session_id
)
select
    s.id,
    s.device,
    s.customer_id,
    ss.duration_sec_count
from sessions s
left join session_stats ss on s.id = ss.session_id
where ss.duration_sec_count > 8
