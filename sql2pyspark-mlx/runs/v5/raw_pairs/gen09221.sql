select s.id, s.device
from sessions s
where s.id in (select session_id from web_events)
order by s.device desc
