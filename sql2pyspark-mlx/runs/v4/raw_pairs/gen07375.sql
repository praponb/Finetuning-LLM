select s.id, count(we.id) as web_event_count
from sessions s
left outer join web_events we on s.id = we.session_id
group by s.id
order by web_event_count desc, id
limit 10
