select page, session_id, count(*) as num_web_events
from web_events
group by page, session_id
having count(*) >= 3
order by num_web_events desc, page, session_id
limit 5;
