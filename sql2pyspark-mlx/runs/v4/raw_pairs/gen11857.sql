select sessions.customer_id, web_events.event_time
from sessions
left join web_events on web_events.session_id = sessions.id
