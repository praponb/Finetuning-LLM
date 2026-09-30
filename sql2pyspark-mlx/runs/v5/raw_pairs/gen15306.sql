select id, duration_sec
from web_events
where duration_sec >= (select avg(duration_sec) from web_events where duration_sec is null)
order by 2 desc, id desc;
