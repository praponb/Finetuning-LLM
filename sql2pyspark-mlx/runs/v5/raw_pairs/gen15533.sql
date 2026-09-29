select id, duration_sec, date_format(event_time, 'yyyy-MM') as event_time_label
from web_events
where session_id in (6, 24, 29, 32)
order by event_time_label;
