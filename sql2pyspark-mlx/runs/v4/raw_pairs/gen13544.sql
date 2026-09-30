select id, session_id, duration_sec
from web_events
where session_id in (18, 32)
