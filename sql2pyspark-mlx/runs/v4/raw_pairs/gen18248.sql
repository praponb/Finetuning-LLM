select s.browser, we.page
from sessions s
left join web_events we on we.session_id = s.id
where we.event_time >= '2023-10-01' or hour(we.event_time) between 8 and 12
