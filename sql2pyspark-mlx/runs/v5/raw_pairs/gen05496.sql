select
    s.id,
    s.browser,
    count(we.id) as web_event_count,
    min(we.event_time) as min_event_time,
    round(avg(we.duration_sec), 2) as avg_duration_sec
from sessions s
left join web_events we on s.id = we.session_id
group by s.id, s.browser
order by 2, id
limit 3;
