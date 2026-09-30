select
    sessions.id,
    sessions.started_at,
    t.product_duration_sec,
    t.total_duration_sec
from sessions
join (select session_id, sum(case when page = '/product' then duration_sec else 0 end) as product_duration_sec, sum(duration_sec) as total_duration_sec
    from web_events
    group by session_id) t on sessions.id = t.session_id
