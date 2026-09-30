select s.country, max(we.duration_sec) as max_duration_sec
from web_events we
left join sessions s on we.session_id = s.id
group by s.country
having max(we.duration_sec) > 310
order by max_duration_sec desc
