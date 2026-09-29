with session_stats as (
    select session_id, count(duration_sec) as duration_sec_count, sum(case when page = '/product' then duration_sec else 0 end) as product_duration_sec
    from web_events
    where event_type = 'page_view'
    group by session_id
)
select
    s.id,
    s.started_at,
    ss.duration_sec_count,
    ss.product_duration_sec
from sessions s
join session_stats ss on s.id = ss.session_id
where ss.duration_sec_count > 9
order by s.started_at
