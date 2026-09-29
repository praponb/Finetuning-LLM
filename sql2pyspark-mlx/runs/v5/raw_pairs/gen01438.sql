select min(duration_sec) as min_duration_sec, avg(duration_sec) as avg_duration_sec
from web_events
order by avg_duration_sec desc;
