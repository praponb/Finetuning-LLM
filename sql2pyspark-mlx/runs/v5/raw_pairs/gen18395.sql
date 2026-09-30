select s.id, s.browser, count(we.id) web_event_count
from sessions s
inner join web_events we on we.session_id = s.id
group by s.id, s.browser
order by s.browser asc;
