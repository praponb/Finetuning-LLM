select s.device, s.country, avg(we.duration_sec) avg_duration_sec
from web_events we
inner join sessions s on we.session_id = s.id
group by s.device, s.country
having avg_duration_sec > 230;
