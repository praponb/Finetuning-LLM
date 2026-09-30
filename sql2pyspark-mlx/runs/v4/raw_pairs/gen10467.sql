select sessions.device, max(web_events.duration_sec) as max_duration_sec, sum(case when web_events.page = '/search' then 1 else 0 end) as search_count
from web_events
left outer join sessions on web_events.session_id = sessions.id
group by sessions.device
having sum(case when web_events.page = '/search' then 1 else 0 end) >= 25
order by device desc
